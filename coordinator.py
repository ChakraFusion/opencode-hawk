#!/usr/bin/env python3
"""
Hawk - a coordinator for long-running local OpenCode sessions.

Watches a local opencode session (e.g. a model served by llama-server through
the llama.cpp provider in the opencode Desktop app). It reads the session
transcript from the opencode SQLite DB and, when the worker has stopped and the
model is idle, issues a self-check "continue" via the opencode CLI attached to
the opencode Desktop's own sidecar server
(`opencode run --attach http://127.0.0.1:<port> --session <id> "...") so the
generation happens inside the Desktop server and the GUI streams it live.

Design: the worker's own judgment decides when it has more to do. A stopped
session gets at most one self-check prompt per re_stall_minutes; a busy turn
or a model that is still generating is never interrupted. The build ends only
on a confirmed STOP: DONE (two distinct messages).

Rule evaluation is idempotent per session message: every message id gets
at most one verdict (continue / escalate), tracked in state.json.

Exit codes: 0 = nothing to do / handled cleanly, 1 = error (DB read etc.),
2 = escalation written (caller may care), 3 = continue injected.

Usages:
    python coordinator.py                 # one poll pass (Task Scheduler)
    python coordinator.py --monitor       # poll loop with --interval minutes
    python coordinator.py --install-task  # print the schtasks command

Environment:
    OPENCODE_BIN   path to the `opencode` CLI (default: derived from PATH)
    OPENCODE_DB    path to opencode.db (default: ~/.local/share/opencode/opencode.db)
    COORD_HOME     dir for config.json / state.json / escalations (default: script dir)
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import secrets
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ── Constants ────────────────────────────────────────────────────────────────
DEFAULT_IDLE_MINUTES = 8
DEFAULT_POLL_MINUTES = 10
# The ONLY completion protocol marker the coordinator acts on. A STOP: DONE
# reply means the whole build plan is finished: the hawk writes a positive
# "done" artifact and stops injecting self-check prompts. Every other marker
# (STOP: VERIFIED / RESULT: PASS / NEEDS_DECISION / …) is no longer a gate —
# the worker's own judgment decides when it has more to do.
PLAN_DONE_MARKERS = ["STOP: DONE", "BUILD PLAN: COMPLETE", "PLAN: COMPLETE",
                     "ALL TASKS COMPLETE", "ALL TASKS DONE"]

CONFIG_DEFAULTS = {
    # Machine-specific paths: MUST be set in config.json on first deploy.
    # Empty defaults are intentional so the code never silently targets a
    # developer's own folders from a fresh clone (see validate_config()).
    "project_dir": "",
    "session_id": "",               # empty = auto-detect newest llama.cpp session; if set and idle for idle_minutes, migrates to the newer project-dir session
    "idle_minutes": DEFAULT_IDLE_MINUTES,
    "poll_minutes": DEFAULT_POLL_MINUTES,
    "stalled_turn_minutes": 15,  # an unfinished turn silent this long counts as stopped
    "auto_accept_external_dirs": True,  # outermost gate for the Desktop permission sweep; a per-session monitored_sessions + auto_accept filter still applies
    "auto_answer_questions": True,  # auto-select the "(Recommended)" option (else first) for pending question-tool asks
    "continue_message": (
        "The user has approved all pending plans and next steps, including "
        "authorization to commit changes as each milestone passes its gates. "
        "No further approval is required. Implement the plan: start with the "
        "next unstarted milestone in the project's plan/spec documents "
        "(SPEC.md, BUILD_PLAN.md, Implementation_Progress.md, and any others "
        "in the repo root) and work through it until its acceptance criteria "
        "are met, your verify gates pass (build, test, lint, fmt), and a "
        "commit exists. Then move on to the following milestones until the "
        "full plan is complete instead of stopping early. Only pause for a "
        "real blocker or a human decision you genuinely need - reply STOP: "
        "BLOCKED or STOP: NEEDS_DECISION for those cases. End with STOP: DONE "
        "only when EVERY milestone and step in the plan is complete."
    ),
    "approve_pending_plans": False,  # grant already inline in continue_message
    "done_confirm_message": (
        "The user has approved all pending plans and next steps, including "
        "authorization to commit changes as each milestone passes its gates. "
        "No further approval is required. Before confirming with STOP: DONE, "
        "re-check the project's plan/spec documents and verify EVERY "
        "milestone and open step is complete: acceptance criteria satisfied, "
        "tests green, commit present. If any step remains open, reply with "
        "what remains to be done instead of STOP: DONE and continue working "
        "on it."
    ),
    "confirm_install_cli": False,
    "restart_desktop_before_continue": False,
    "desktop_exe": "",              # path to the opencode Desktop exe (restart_desktop_before_continue only); empty = never restart
    "continue_via_attach": True,  # prefer Desktop sidecar so the GUI streams live
    "llama_pid": 0,               # 0 = auto-detect the llama-server process by name
    "llama_api_port": 1234,       # where llama-server serves its OpenAI API (/slots, /metrics)
    # llama_idle probe internals — used only to stand down the self-check while
    # a long generation is live (a long decode writes no session parts yet).
    "stall_llama_cpu_frac": 0.10, # min CPU-fraction over the watch window to count as "generating"
    "stall_llama_window_s": 360,  # how far back to measure llama CPU activity (must be > poll interval so the cross-poll delta fires)
    "stall_gate_slots": True,     # when CPU looks idle, also consult llama-server /slots is_processing
    "stall_gate_gpu": True,       # when CPU looks idle, also consult per-process GPU utilisation
    "stall_gate_tokens": True,    # when CPU looks idle, also require output+ingest token speeds to both be zero
    "stall_llama_gpu_util": 10.0, # min GPU-util % (across llama's engines) to count as "generating"
    "llama_gpu_cache_s": 5.0,     # how long to cache a GPU-util sample in state
    "llama_task_tracking": True,  # hawk captures llama-server task ids for provider-truth activity (design 2026-09-23); False disables the tailer thread
    "stall_idle_confirm_gap_s": 30,  # re-check llama busy-ness this many seconds after the first busy reading; busy only counts if BOTH samples are busy (kills short own-harness blips on the shared server)
    "gpu_vram_total_mb": 16384,   # GPU VRAM total (MB) that pins the VRAM chart Y-axis + "used/total" %. 0 = auto-detect via DXGI. Default matches the RX 9060 XT (16 GB).
    "re_stall_minutes": 45,     # min gap between self-check prompts for a session
                                # that keeps stopping without replying STOP: DONE
    "record_escalations_to_session": True,  # also write each escalation into the session transcript DB
    "monitored_sessions": {},             # per-session steering: {sid: {enabled, auto_accept, auto_continue, project_dir}}
    # Outbound alerts. Channel:
    #   notify.ntfy {"server": "https://ntfy.sh", "topic": "...", "priority": 3,
    #                 "devices": [{"id": "...", "name": "...", "note": ""},
    #                 "kinds": [...], "kind_cooldown_s": {kind: secs}}
    # kinds gates PHONE pushes only; a missing/absent key falls back to
    # notify.DEFAULT_NOTIFY_KINDS at read time (config is top-level merged,
    # never deep-merged). The web UI always shows every event.
    "notify": {"ntfy": {"kinds": ["escalation", "plan_done", "test"],
                        "kind_cooldown_s": {"commit": 60, "continue": 60,
                                            "nudge": 60}}},
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ts_str(ms) -> str:
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ms)


# Append handle for home_dir()/"monitor.log", keyed by the resolved path so a
# COORD_HOME change or a monkeypatched home_dir() re-opens instead of writing to
# the previous home. At most one entry is live: the process has one home at a
# time, and holding an open handle to a home we no longer write to is a leak.
_LOG_FH_CACHE: dict = {}


def _close_quiet(fh) -> None:
    # Closing a cached log handle is best-effort: the handle may already be
    # closed (harmless), or -- in a test-injected fault -- not have a close()
    # at all. Swallow here so no close attempt can ever escape log().
    try:
        fh.close()
    except Exception:
        pass


def log(msg: str) -> None:
    # Two sinks, on purpose. The console print is the trail the launchers
    # (quickstart.bat, _launch_monitor.py) show the human; the append is the
    # trail that survives whatever the launcher pointed stdout at, and it is
    # what dashboard.log_path() tails. stdout alone loses the trail entirely
    # when the process is started with stdout redirected elsewhere.
    line = "%s | %s" % (utcnow().strftime("%H:%M:%S"), msg)
    print(line, flush=True)
    if "--self-test" in sys.argv:
        # The self-test is a synthetic harness, and its log() calls outside the
        # narrow COORD_HOME/home_dir patch windows resolve home_dir() to the
        # real repo -- so appending writes "SELF-CHECK injected" / "ESCALATE: ..."
        # into the production trail that dashboard.log_path() tails, poisoning
        # the very file this card repairs, on every gate run. main() dispatches
        # --self-test to run-and-exit, so a process carrying it can never be the
        # live monitor. Blind spot: this is argv sniffing only, so an in-process
        # call with no flag (python -c "import coordinator; coordinator.self_test()",
        # or a third-party harness such as selftest_notify.py, which is safe only
        # because it monkeypatches hawk.home_dir itself) still appends.
        return
    try:
        # Resolve the cache key first, behind its own guard: everything after
        # this point needs `key`, and an exception raised in an except handler's
        # body is NOT caught by that same handler -- so an unbound `key` here
        # would escape log() as an UnboundLocalError.
        key = os.path.realpath(str(home_dir() / "monitor.log"))
    except Exception:
        return            # cannot even resolve the path: nothing to append to
    try:
        # Skip the append when stdout already IS monitor.log: the canonical
        # launcher hands the child an open append handle, so the print above
        # has just written this line and appending again would duplicate every
        # line. Unknown (no fileno, or the file does not exist yet) is not the
        # same file, so we append.
        try:
            if os.path.samefile(sys.stdout.fileno(), key):
                return
        except Exception:
            pass
        fh = _LOG_FH_CACHE.get(key)
        if fh is None:
            # "a" + encoding="utf-8" + buffering=1: real line buffering (in
            # binary mode buffering=1 warns and is ignored), non-ASCII messages
            # round-trip, and the platform's own line ending is what lands in
            # the file -- the same CRLF the console trail and the rest of
            # monitor.log already use.
            fh = open(key, "a", encoding="utf-8", buffering=1)
            stale = [_LOG_FH_CACHE.pop(k) for k in list(_LOG_FH_CACHE)]
            _LOG_FH_CACHE[key] = fh
            for old in stale:
                _close_quiet(old)
        fh.write(line + "\n")
        fh.flush()
    except Exception:
        # The append must never raise and never mask the exception being handled:
        # log() is called from ~62 sites, ~25 of them inside except handlers, and a
        # locked or rotated log must not take down a caller. The failures are not
        # just OSError -- a stale closed handle raises ValueError, a bad codec
        # raises UnicodeEncodeError -- hence the blanket except. Drop the handle
        # so the next call re-opens and re-attempts.
        bad = _LOG_FH_CACHE.pop(key, None)
        if bad is not None:
            _close_quiet(bad)


def log_debug(msg: str) -> None:
    if os.environ.get("COORD_DEBUG"):
        log("DEBUG " + msg)


# ── State / config ───────────────────────────────────────────────────────────
def home_dir() -> Path:
    return Path(os.environ.get("COORD_HOME") or Path(__file__).resolve().parent)


def load_json(path: Path, fallback):
    # save_json swaps the file in with os.replace, so a concurrent reader can
    # transiently fail (on Windows, a sharing violation) on a perfectly good
    # file. Retry before falling back: the fallback is CONFIG_DEFAULTS, whose
    # monitored_sessions is {} -- accepting it silently discards the user's real
    # settings, and any save in that same cycle then persists the loss.
    err = None
    for attempt in range(5):
        if not path.exists():
            return json.loads(json.dumps(fallback)) if isinstance(fallback, dict) else fallback
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                return json.load(f)
        except Exception as e:
            err = e
            time.sleep(0.05 * (attempt + 1))
    log("warning: could not read %s (%s); falling back to defaults" % (path, err))
    return json.loads(json.dumps(fallback)) if isinstance(fallback, dict) else fallback


def save_json(path: Path, obj) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
    # On Windows os.replace loses to a concurrent reader with a sharing
    # violation, so retry instead of letting the write vanish: callers treat a
    # raised error as "not saved" and the dashboard's fetch swallows it, which
    # shows up as a settings toggle that silently does nothing.
    for attempt in range(8):
        try:
            tmp.replace(path)
            return
        except OSError:
            if attempt == 7:
                raise
            time.sleep(0.05 * (attempt + 1))


def load_config() -> dict:
    cfg = load_json(home_dir() / "config.json", CONFIG_DEFAULTS)
    for k, v in CONFIG_DEFAULTS.items():
        cfg.setdefault(k, v)
    return cfg


def ensure_ntfy_topic(cfg: dict, persist: bool = True) -> str | None:
    """Give a fresh install its own ntfy topic. An ntfy.sh topic name works
    like a password (anyone who knows it can read and publish), so every
    install gets "hawk-" plus 12 random digits instead of a shared default.
    Returns the new topic when one was generated (and saved to config.json
    when persist is set), else None. An existing topic is never touched."""
    n = cfg.setdefault("notify", {})
    if not isinstance(n, dict):
        return None
    ntfy = n.setdefault("ntfy", {})
    if not isinstance(ntfy, dict) or str(ntfy.get("topic") or "").strip():
        return None
    topic = "hawk-%012d" % secrets.randbelow(10 ** 12)
    ntfy["topic"] = topic
    if persist:
        save_json(home_dir() / "config.json", cfg)
    return topic


def validate_config(cfg: dict) -> list[str]:
    """Return one-line config problems worth telling the operator about. The
    coordinator still runs with a broken config (rules degrade gracefully), so
    these are warnings logged at startup, not fatal errors."""
    problems = []
    if not (cfg.get("project_dir") or "").strip():
        problems.append("config: project_dir is empty - set it in config.json "
                        "(git commit + dirty-tree checks will be skipped)")
    if (cfg.get("restart_desktop_before_continue") or False) \
            and not (cfg.get("desktop_exe") or "").strip():
        problems.append("config: restart_desktop_before_continue is on but "
                        "desktop_exe is empty - restarts will never fire")
    n = cfg.get("notify") or {}
    ntfy = n.get("ntfy") or {}
    if ntfy and not str(ntfy.get("topic") or "").strip():
        problems.append("config: notify.ntfy is present but topic is missing - "
                        "ntfy alerts will not send")
    return problems


def load_state() -> dict:
    return load_json(home_dir() / "state.json", {})


def save_state(state: dict) -> None:
    save_json(home_dir() / "state.json", state)


PER_STATE_DEFAULTS = {
    "last_evaluated_mid": "", "prev_head": "", "continues": 0,
    "last_continue_at": 0, "last_seen_at": 0, "pending_done_mid": "",
    "done_mid": "",
}


def monitored_roots(cfg) -> list[str]:
    ms = cfg.get("monitored_sessions") or {}
    return [sid for sid, e in ms.items() if e and e.get("enabled")]


def _single_session_enabled(cfg, cli_override: bool = False) -> bool:
    """Legacy single-session fallback: monitor `session_id` only when the
    dashboard hasn't explicitly disabled it. A session listed in
    `monitored_sessions` with `enabled: false` must NOT be auto-continued even
    when it is the configured session_id (the web UI checkbox is the user's
    explicit on/off). A session with no entry at all keeps the old behaviour
    (any configured session_id is monitored). `cli_override=True` (a manual
    `--session-id`) always permits the single-session run."""
    if cli_override:
        return True
    sid = (cfg.get("session_id") or "").strip()
    if not sid:
        return False
    e = (cfg.get("monitored_sessions") or {}).get(sid)
    if e is None:
        return True
    return bool(e.get("enabled"))


def per_state(state: dict, sid: str) -> dict:
    per = state.setdefault("per", {})
    return per.setdefault(sid, copy.deepcopy(PER_STATE_DEFAULTS))


def control_file() -> Path:
    return home_dir() / "control.json"


def read_control() -> dict:
    try:
        p = control_file()
        if p.exists():
            with open(p, "r", encoding="utf-8-sig") as f:
                d = json.load(f)
            return d if isinstance(d, dict) else {}
    except Exception as e:
        log_debug("read_control: %s" % e)
    return {}


def apply_control(cfg, state) -> bool:
    """Consume a pending dashboard intent (pause/resume/poll_now), seq-guarded.
    Returns True if a poll_now was delivered (caller should poll immediately)."""
    ctrl = read_control()
    if not ctrl:
        return False
    try:
        seq = int(ctrl.get("seq") or 0)
        if seq <= int(state.get("control_seq") or 0):
            return False
        intent = str(ctrl.get("intent") or "")
    except Exception as e:
        log_debug("apply_control: %s" % e)
        return False
    state["control_seq"] = seq
    if intent == "pause":
        state["paused"] = True
        log("control: PAUSE")
    elif intent == "resume":
        state["paused"] = False
        log("control: RESUME")
    elif intent == "poll_now":
        state["last_poll_now"] = int(time.time() * 1000)
        log("control: POLL NOW")
        save_state(state)
        return True
    save_state(state)
    return False


def db_path() -> Path:
    p = os.environ.get("OPENCODE_DB")
    if p:
        return Path(p)
    return Path.home() / ".local" / "share" / "opencode" / "opencode.db"


# ── DB access ────────────────────────────────────────────────────────────────
def connect_db(path: Path, attempts=5):
    # A missing file is not transient (unlike a locked DB): fail at once
    # instead of spending ~7.5 s in retries on every dashboard request.
    if not Path(path).exists():
        raise RuntimeError("cannot open opencode DB %s: file not found" % path)
    uri = "file:" + str(path).replace("\\", "/") + "?mode=ro"
    last = None
    for i in range(attempts):
        try:
            con = sqlite3.connect(uri, uri=True, timeout=10)
            con.execute("PRAGMA busy_timeout=8000")
            con.execute("SELECT 1")  # verify readable
            return con
        except sqlite3.Error as e:
            last = e
            time.sleep(0.5 * (i + 1))
    raise RuntimeError("cannot open opencode DB %s: %s" % (path, last))


def select_session(con, cfg) -> dict | None:
    sid = (cfg.get("session_id") or "").strip()
    if sid:
        row = con.execute(
            "SELECT id, title, model, directory, time_updated FROM session WHERE id=?",
            (sid,),
        ).fetchone()
        if not row:
            raise RuntimeError("configured session_id not found: %s" % sid)
        base = dict(id=row[0], title=row[1], model=row[2], directory=row[3], time_updated=row[4])
        # Auto-follow the live session: if a different project-dir session has
        # newer activity and the configured one has been idle past the idle
        # window, migrate session_id (persist) so idle-session resumes land in
        # the session the worker is actually using.
        cand = newest_project_session(con, cfg, sid)
        if cand and cand["time_updated"] > base["time_updated"]:
            idle_ms = int(cfg.get("idle_minutes", DEFAULT_IDLE_MINUTES)) * 60_000
            if int(time.time() * 1000) - base["time_updated"] >= idle_ms:
                log("session_id migrated %s -> %s (config session idle; %r has newer activity)"
                    % (sid, cand["id"], (cand["title"] or "")[:60]))
                cfg["session_id"] = cand["id"]
                save_json(home_dir() / "config.json", cfg)
                return cand
        return base

    proj_dir = (cfg["project_dir"] or "").lower().replace("\\", "/").rstrip("/")
    rows = con.execute(
        "SELECT id, title, model, directory, time_updated FROM session "
        "ORDER BY time_updated DESC"
    ).fetchall()
    for r in rows:
        model = (r[2] or "")
        try:
            mj = json.loads(model) if isinstance(model, str) else model
            provider = mj.get("providerID", "") if isinstance(mj, dict) else ""
        except Exception:
            provider = ""
        if provider != "llama.cpp":
            continue
        d = (r[3] or "")
        if proj_dir and d and d.lower().replace("\\", "/").rstrip("/") != proj_dir:
            continue
        log("watching session %s (%s) dir=%s" % (r[0], (r[1] or "")[:60], d))
        return dict(id=r[0], title=r[1], model=r[2], directory=r[3], time_updated=r[4])
    raise RuntimeError("no llama.cpp session found in project dir %s" % cfg["project_dir"])


def newest_project_session(con, cfg, exclude_sid: str) -> dict | None:
    """Most recently touched session in the project dir (any provider, with at
    least one message) other than exclude_sid, or None. Used to auto-follow the
    live worker session when the configured one has gone idle."""
    proj_dir = (cfg["project_dir"] or "").lower().replace("\\", "/").rstrip("/")
    rows = con.execute(
        "SELECT id, title, model, directory, time_updated FROM session "
        "WHERE id != ? ORDER BY time_updated DESC LIMIT 50",
        (exclude_sid,)).fetchall()
    for r in rows:
        if proj_dir:
            d = (r[3] or "").lower().replace("\\", "/").rstrip("/")
            if d != proj_dir:
                continue
        n = con.execute(
            "SELECT COUNT(*) FROM message WHERE session_id=?", (r[0],)).fetchone()[0]
        if n == 0:
            continue
        return dict(id=r[0], title=r[1], model=r[2], directory=r[3], time_updated=r[4])
    return None


def parse_part_data(raw):
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return {}
    return raw if isinstance(raw, dict) else {}


def get_text(d: dict) -> str:
    t = d.get("text")
    if isinstance(t, str):
        return t
    if isinstance(t, list):
        out = []
        for b in t:
            if isinstance(b, str):
                out.append(b)
            elif isinstance(b, dict):
                out.append(str(b.get("text", "")))
        return "".join(out)
    return ""


def session_parts(con, sid):
    rows = con.execute(
        "SELECT p.time_created, p.data, p.message_id, m.data "
        "FROM part p LEFT JOIN message m ON p.message_id = m.id "
        "WHERE p.session_id=? ORDER BY p.time_created", (sid,)
    ).fetchall()
    out = []
    for t, d, mid, md in rows:
        pd = parse_part_data(d)
        role = None
        if md is not None:
            mm = parse_part_data(md)
            if isinstance(mm, dict):
                role = mm.get("role")
        out.append({"time": t, "message_id": mid, "role": role,
                    "type": pd.get("type"), "text": get_text(pd),
                    "data": pd})
    return out


def session_messages(con, sid):
    rows = con.execute(
        "SELECT id, time_created, data FROM message WHERE session_id=? ORDER BY time_created",
        (sid,),
    ).fetchall()
    out = []
    for mid, t, d in rows:
        md = parse_part_data(d)
        if not isinstance(md, dict):
            continue
        out.append({"id": mid, "time": t, "role": md.get("role"),
                    "company": md.get("modelID"),
                    "time_data": md.get("time") or {}, "raw": md})
    return out


def session_todos(con, session_id: str) -> list[dict]:
    """Unchecked (active) task list for the session: any todo whose status is
    still pending/in_progress (i.e. not completed/cancelled/archived)."""
    ACTIVE = ("pending", "in_progress")
    rows = con.execute(
        "SELECT content, status, priority FROM todo "
        "WHERE session_id=? AND status IN ('pending','in_progress') ORDER BY position",
        (session_id,),
    ).fetchall()
    return [{"content": c, "status": s, "priority": p} for c, s, p in rows]


def session_todo_total(con, session_id: str) -> int:
    """Total tasks tracked in the plan (ALL statuses, not just active). This lets
    the rule engine tell 'the whole plan is checked off' apart from 'no tasks
    are tracked at all' — the two cases both show zero open tasks."""
    row = con.execute(
        "SELECT COUNT(*) FROM todo WHERE session_id=?", (session_id,)).fetchone()
    return int(row[0]) if row else 0


def last_part_time(parts) -> int:
    return max((p["time"] for p in parts), default=0)


def busy_from_messages(messages) -> bool:
    if not messages:
        return False
    last = messages[-1]
    td = last["time_data"] or {}
    if "completed" not in td:
        return True  # the latest assistant turn has not finished -> still generating
    return False


def await_injected_message_id(session_id, newer_than_ms,
                              attempts=10, wait_s=1.0) -> str:
    """After a fire-and-forget continue, find the user message the injected CLI
    created (the prompt lands in the DB moments later). Returns its id or ''."""
    db = db_path()
    if not db.exists():
        return ""
    for _ in range(attempts):
        try:
            with connect_db(db) as con:
                for m in session_messages(con, session_id):
                    if m.get("role") == "user" and m["time"] > int(newer_than_ms):
                        return m["id"]
        except Exception as e:
            log_debug("await_injected_message_id: %s" % e)
        time.sleep(wait_s)
    return ""


def session_row(con, sid: str) -> dict | None:
    row = con.execute(
        "SELECT id, title, model, directory, time_updated FROM session WHERE id=?",
        (sid,)).fetchone()
    if not row:
        return None
    return dict(id=row[0], title=row[1], model=row[2], directory=row[3],
                time_updated=row[4])


def active_leaf(con, root: dict, max_depth: int = 3) -> dict:
    """Deepest descendant of `root` (via session.parent_id) whose last message
    has no 'completed' mark (a live sub-agent turn). Returns the root itself
    when no child qualifies."""
    node = root
    for _ in range(max_depth):
        rows = con.execute(
            "SELECT id, title, model, directory, time_updated FROM session "
            "WHERE parent_id=? ORDER BY time_updated DESC LIMIT 1",
            (node["id"],)).fetchall()
        if not rows:
            break
        child = dict(id=rows[0][0], title=rows[0][1], model=rows[0][2],
                     directory=rows[0][3], time_updated=rows[0][4])
        msgs = session_messages(con, child["id"])
        if not msgs or busy_from_messages(msgs):
            node = child  # child's latest turn is unfinished -> it is the leaf
            continue
        break
    return node


def node_growth(parts, per: dict) -> bool:
    """True when the transcript gained a part after the last time we saw it."""
    if not parts:
        return False
    return last_part_time(parts) > int(per.get("last_seen_at") or 0)


def progress_since(parts, since_ms) -> bool:
    return any(p["time"] > since_ms for p in parts)


# ── Transcript analysis ──────────────────────────────────────────────────────
def analyze(parts, messages):
    """Return dict with the signals the self-check model still uses: is the
    worker's most recent *assistant* text a plan-done marker (STOP: DONE), plus
    the transcript strings the escalation / plan-done artifacts embed.

    Only role=assistant text parts are read (parts without a role - legacy rows,
    test fixtures - are treated as assistant). Injected coordinator prompts are
    user messages and must never be mistaken for the worker's protocol marker."""
    tail = parts[-12:]
    tool_tail = [p for p in tail if p["type"] == "tool"]

    tool_blob = ""
    for p in tool_tail:
        d = p["data"]
        out = d.get("output")
        if isinstance(out, str):
            tool_blob += "\n" + out[:1500]
        elif isinstance(out, list):
            for b in out:
                if isinstance(b, dict):
                    tool_blob += "\n" + str(b.get("text", ""))[:1500]

    # Roll up assistant text parts per message id. The newest assistant message
    # whose text we can see is the worker's last word.
    msg_text = {}
    last_mid = None
    for p in parts:
        if p.get("role") == "user":
            continue
        if p.get("type") != "text" or not p.get("text"):
            continue
        mid = p.get("message_id")
        msg_text.setdefault(mid, []).append(p["text"])
        last_mid = mid

    last_text = "".join(msg_text.get(last_mid) or [])

    plan_done_marker = next((m for m in PLAN_DONE_MARKERS if m in last_text), "")

    return {
        "last_assistant": last_text,
        "plan_done": bool(plan_done_marker),
        "plan_done_marker": plan_done_marker,
        "plan_done_mid": last_mid if plan_done_marker else "",
        "markers_hit": [plan_done_marker] if plan_done_marker else [],
        "tool_blob": tool_blob,
        "last_part_time": last_part_time(parts),
        "has_text": bool(last_text),
    }


# Option enumeration shared with notify.py's mobile buttons. The auto-answer
# decision logic that sat on top of it is gone with the old rule engine.
_OPTION_RE = re.compile(
    r"^\s*(?:[\-\u2022\*]|\d+[.)]|[a-zA-Z][.)]|\(\d+\)|[a-zA-Z]\))\s+(.+)$")


def extract_choices(text: str) -> list[str]:
    out = []
    for line in (text or "").splitlines():
        m = _OPTION_RE.match(line)
        if m:
            item = m.group(1).strip().rstrip(".")
            if item:
                out.append(item)
    return out if len(out) >= 2 else []


# ── Git & test gates ─────────────────────────────────────────────────────────
def creation_flags() -> int:
    """Prevent flashing console windows for children of the (no-window)
    monitor/dashboard processes on Windows."""
    if os.name == "nt":
        return (getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
                | getattr(subprocess, "DETACHED_PROCESS", 0x00000008)
                | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200))
    return 0


def git(project_dir, *args):
    res = subprocess.run(
        ["git", "-C", project_dir, *args],
        capture_output=True, text=True, encoding="utf-8",
        errors="replace", timeout=60,
        creationflags=creation_flags(),
    )
    return res.returncode, res.stdout.rstrip(), res.stderr.rstrip()


def git_head(project_dir) -> str:
    rc, out, _ = git(project_dir, "rev-parse", "HEAD")
    return out if rc == 0 else ""


def project_test_command(project_dir):
    """Pick the test-gate invocation for a project by its de-facto shape.
    Cargo.toml -> cargo test (Rust tree); dashboard.py -> the hawk dashboard's
    own --self-test (Python); otherwise None = no gate applies to this tree."""
    root = Path(project_dir)
    if (root / "Cargo.toml").exists():
        return (["cargo", "test"], "cargo")
    if (root / "dashboard.py").exists():
        return ([sys.executable, "-X", "utf8", "dashboard.py", "--self-test"],
                "selftest")
    return (None, None)


def run_tests(project_dir, timeout, max_attempts=3, lock_wait_s=10) -> dict:
    """Run the gate tests for a project. Cargo runs retry on build-lock
    contention (concurrent `cargo test` from a sibling worker would otherwise
    surface as an unparseable result); non-Rust trees use their own runner."""
    cmd, kind = project_test_command(project_dir)
    if cmd is None:
        return {"ok": True, "passed": 0, "failed": -1,
                "note": "no test runner detected (gate skipped)"}
    log("running %s (gate)..." % " ".join(cmd))
    for attempt in range(1, max_attempts + 1):
        try:
            res = subprocess.run(
                cmd, cwd=project_dir, capture_output=True, text=True,
                timeout=timeout, creationflags=creation_flags(),
            )
            out = res.stdout + "\n" + res.stderr
        except subprocess.TimeoutExpired:
            return {"ok": False, "passed": 0, "failed": -1,
                    "note": "timeout (%ds)" % timeout}
        if kind == "cargo":
            m = re.search(r"test result: ok\.\s+(\d+) passed;\s*(\d+) failed", out)
            if m:
                passed, failed = int(m.group(1)), int(m.group(2))
                return {"ok": failed == 0, "passed": passed,
                        "failed": failed, "note": ""}
            if "test result: FAILED" in out or re.search(r"error\[E\d", out):
                return {"ok": False, "passed": 0, "failed": -1,
                        "note": "compile/test failure"}
            if attempt < max_attempts and re.search(
                    r"Blocking waiting for file lock", out, re.I):
                log("cargo test lock contention (attempt %d/%d); "
                    "retrying in %ds" % (attempt, max_attempts, lock_wait_s))
                time.sleep(lock_wait_s)
                continue
            return {"ok": False, "passed": 0, "failed": -1,
                    "note": "unparseable result"}
        # kind == "selftest": dashboard.py --self-test
        if res.returncode == 0 and "self-test: PASS" in out:
            return {"ok": True, "passed": 1, "failed": -1,
                    "note": "selftest PASS"}
        if "self-test: FAIL" in out:
            return {"ok": False, "passed": 0, "failed": -1,
                    "note": "selftest FAIL"}
        return {"ok": False, "passed": 0, "failed": -1,
                "note": "selftest unparseable"}


# ── Continue injection ───────────────────────────────────────────────────────
def restart_desktop(cfg) -> bool:
    """Kill + relaunch the opencode Desktop app between milestones.

    The hawk becomes the single driver once the (plugin-loading) Desktop is
    restarted; this runs after a milestone is verified and BEFORE the next
    continue is injected, so the running session is never interrupted mid-turn.
    Returns True if a restart happened, False if skipped (no exe / not running).
    """
    exe = (cfg.get("desktop_exe") or "").strip()
    if not exe or not os.path.exists(exe):
        log("desktop restart skipped: desktop_exe not found (%r)" % exe)
        return False
    tl = subprocess.run(["tasklist", "/FI", "IMAGENAME eq OpenCode.exe"],
                        capture_output=True, text=True, creationflags=creation_flags())
    if "OpenCode.exe" not in tl.stdout:
        log("desktop restart skipped: OpenCode.exe not running")
        return False
    log("restarting OpenCode Desktop (between milestones)...")
    subprocess.run(["taskkill", "/IM", "OpenCode.exe", "/F"],
                   capture_output=True, text=True, creationflags=creation_flags())
    time.sleep(4)
    flags = creation_flags()
    subprocess.Popen([exe], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                     stderr=subprocess.DEVNULL, close_fds=True, creationflags=flags)
    time.sleep(8)
    log("OpenCode Desktop restarted")
    return True


def find_opencode() -> str | None:
    if os.environ.get("OPENCODE_BIN"):
        p = Path(os.environ["OPENCODE_BIN"])
        if p.exists():
            return str(p)
        log("warning: OPENCODE_BIN=%s does not exist" % p)
    exe = shutil.which("opencode")
    return exe


def find_desktop_server() -> str | None:
    """Return the opencode Desktop sidecar URL (http://127.0.0.1:PORT) if one is
    listening, else None. Locale-independent netstat/tasklist parsing."""
    try:
        out = subprocess.run(["tasklist", "/FI", "IMAGENAME eq OpenCode.exe", "/FO", "CSV", "/NH"],
                             capture_output=True, timeout=15,
                             creationflags=creation_flags()).stdout.decode("cp1252", errors="replace")
        pids = {int(m) for m in re.findall(r'"OpenCode\.exe","(\d+)"', out)}
        if not pids:
            return None
        out = subprocess.run(["netstat", "-ano"], capture_output=True, timeout=20,
                             creationflags=creation_flags()).stdout.decode("cp1252", errors="replace")
        for line in out.splitlines():
            t = line.split()
            if len(t) >= 5 and t[0] == "TCP" and t[1].startswith("127.0.0.1:"):
                foreign = t[2].replace("0.0.0.0:0", "").replace("[::]:0", "").strip()
                if foreign and foreign != "0" and ":" in foreign:
                    continue  # not a listener
                if t[-1].isdigit() and int(t[-1]) in pids:
                    return "http://" + t[1]
    except Exception as e:
        log_debug("find_desktop_server: %s" % e)
    return None


# ── Desktop permission auto-accept ───────────────────────────────────────────
_PERM_REPLIED = {}  # requestID -> timestamp of last reply (dedupe window 15s)


def record_permission_accept(rid, ver, session_id, target, kind="permission_accept"):
    """Append one JSONL line to home_dir()/permission_events.jsonl for the
    dashboard's event timeline. Kept separate from the session DB so the fed
    transcript (and thus the LLM context) is never touched by sidecar actions.
    Never raises — the event feed is optional."""
    try:
        ev = {"t": int(time.time() * 1000), "kind": kind,
              "id": rid, "ver": ver, "session": session_id or "",
              "target": (target or "")[:240]}
        with open(home_dir() / "permission_events.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    except Exception:
        pass


def desktop_sidecar():
    """Discover the opencode Desktop sidecar as (base_url, creds).

    The Desktop bundles its opencode server behind HTTP Basic auth with a
    per-launch generated username/password exposed only in the OpenCode.exe
    process environment, so read them fresh at call time; never persist them.
    """
    try:
        import psutil
    except ImportError:
        log_debug("desktop_sidecar: psutil not installed")
        return None
    sidecar = find_desktop_server()
    if not sidecar:
        return None
    user = pwd = None
    for proc in psutil.process_iter(["name"]):
        if proc.info.get("name") != "OpenCode.exe":
            continue
        try:
            env = proc.environ()
        except Exception:
            continue
        u = env.get("OPENCODE_SERVER_USERNAME")
        p = env.get("OPENCODE_SERVER_PASSWORD")
        if u and p:
            user, pwd = u, p
            break
    if not (user and pwd):
        log_debug("desktop_sidecar: no OpenCode.exe server credentials in env")
        return None
    return sidecar, {"username": user, "password": pwd}


def _perm_request(base, creds, path, method="GET", body=None, timeout=8):
    import base64
    import urllib.request
    auth = "Basic " + base64.b64encode(
        ("%s:%s" % (creds["username"], creds["password"])).encode("utf-8")).decode("ascii")
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method, headers={
        "Authorization": auth,
        "Content-Type": "application/json" if data is not None else "text/plain"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception as e:
        return -1, (str(e)).encode("utf-8", "replace")


PERM_AUTO_ACTIONS = ("external_directory",)
# Auto-accept scope bounds (plan §4a). Depth matches active_leaf's, so both
# walkers agree on what "the session tree" means.
PERM_LINEAGE_MAX_DEPTH = 3   # parent hops walked up session.parent_id
PERM_SUBDIR_FANOUT = 16      # children read per lineage node
PERM_SUBDIR_NODES = 400      # lineage nodes discovered per sweep
PERM_DIR_CAP = 128           # directories returned by permission_directories


def _perm_action(req: dict) -> str:
    """Name of the thing being asked for. v2 calls it 'action', v1 'permission'."""
    if not isinstance(req, dict):
        return ""
    return str(req.get("action") or req.get("permission") or "")


def _perm_json(base, creds, path):
    """GET a sidecar route and parse JSON, or None when the route is absent.

    Unknown /api/* paths are proxied to app.opencode.ai and come back as 403
    HTML rather than 404, so a non-200 or non-JSON body means "this build does
    not serve that route" -- never "the queue is empty"."""
    st, raw = _perm_request(base, creds, path)
    if st != 200:
        log_debug("permission GET %s -> HTTP %s" % (path, st))
        return None
    try:
        return json.loads(raw.decode("utf-8", "replace"))
    except Exception:
        log_debug("permission GET %s -> non-JSON body %r" % (path, raw[:80]))
        return None


def _open_session_db():
    """One best-effort read-only handle on the opencode session DB, or None.

    None is a normal answer, not an error. The exists() guard keeps a missing DB
    free (connect_db retries 5x with a 0.5*(i+1)s backoff), and a DB that exists
    but will not open -- locked past busy_timeout, mid-recovery -- is swallowed
    too, so one unreadable DB can never take a whole sweep down. Callers must
    read None as "unprovable" and fail closed."""
    try:
        sdb = db_path()
        return connect_db(sdb) if sdb.exists() else None
    except Exception as e:
        log_debug("permission: open session DB: %s" % e)
        return None


def perm_root_of(con, sid: str, max_depth: int = PERM_LINEAGE_MAX_DEPTH) -> str | None:
    """Walk `sid` up session.parent_id to its root session id.

    Returns None -- never raises -- when the id is empty, the row is unknown,
    the chain is longer than max_depth, a cycle is seen, or the query fails.
    None means "unprovable", and every caller must read that as "do not
    auto-accept". The only DB call is wrapped, and every other operation is on
    a value already narrowed to str/int, so the sole caller (which passes the
    default bound) cannot be handed an exception to propagate. A dedicated
    one-column PK walk: session_row (:536) does not select parent_id and must not
    grow one (4 callers, 4 fake tables)."""
    cur = sid.strip() if isinstance(sid, str) else ""
    seen = set()
    for _ in range(max_depth + 1):  # +1 so a 3-deep chain still reaches its root
        if not cur or cur in seen:
            return None  # nameless ask, or a parent_id cycle (R6)
        seen.add(cur)
        try:
            row = con.execute("SELECT parent_id FROM session WHERE id=?",
                              (cur,)).fetchone()
        except Exception as e:
            log_debug("perm_root_of(%s): %s" % (cur, e))
            return None
        if row is None:
            return None  # no such row: lineage unprovable (AC5)
        parent = row[0].strip() if isinstance(row[0], str) else ""
        if not parent:
            return cur  # root reached
        cur = parent
    return None  # deeper than the bound (R7): fails closed, ask stays hanging


def perm_session_allowed(cfg, sid: str, con=None) -> bool:
    """True iff `sid` is, or descends from, a `monitored_sessions` key whose
    auto_accept is on. `enabled` is hawk's steering toggle and is NOT consulted.

    Fails closed, and "provable" admits no exceptions: the grant set is a
    function of the session table, never of whether the DB happened to open this
    cycle. `con=None` is a HARD fail-closed sentinel -- it never means "open your
    own", and no code path does -- so it rejects every sid, registered or not,
    and says why under COORD_DEBUG. A registered key is judged by its own entry
    only when the DB PROVES it is a root (parent_id NULL), never because the DB
    could not be read: 32 of the 46 registered keys on this machine are
    subagents, so a missing handle that judged one by its own stale auto_accept
    would re-admit exactly the sessions a root's revocation exists to keep
    rejected, and would do it with durable grants (AC4(d), AC5(i))."""
    if not isinstance(sid, str) or not sid.strip():
        return False  # must not accept a nameless ask
    ms = cfg.get("monitored_sessions") or {}
    if not isinstance(ms, dict):
        return False
    if con is None:
        log_debug("perm_session_allowed(%s): no session DB handle this cycle; "
                  "rejecting, lineage unprovable" % sid)
        return False  # DB unavailability must never widen the grant set
    # perm_root_of never raises (its docstring's contract: the one DB call is
    # wrapped, everything else operates on a value already narrowed to str),
    # so there is nothing to catch here -- only a result to distrust.
    root = perm_root_of(con, sid)
    # Unprovable lineage -- no row, a cycle, deeper than the bound, or a
    # transient DB error -- comes back None and REJECTS (AC5, R6, R7). Never
    # read a missing row as "its own root": a transient error would then
    # turn into a durable "always" grant with zero lineage proof.
    entry = ms.get(root) if root else None
    return isinstance(entry, dict) and bool(entry.get("auto_accept", True))


def permission_directories(cfg, base, creds, con=None) -> list[str]:
    """Directories to sweep, in order: the `/project` worktrees, cfg["project_dir"],
    each monitored session's own project_dir, then each monitored session's own
    session directory and its sub-agents' (root-first, capped at PERM_DIR_CAP).

    Both pending-permission list routes are scoped to one directory and default
    to the server's own project, so asks raised in another project (e.g.
    demo_project) are only visible when the directory is passed explicitly --
    and a session (or a sub-agent spawned under it) can work somewhere that is
    neither a /project worktree nor in the config, so its directory has to be
    discovered too or its asks sit unanswered. `enabled` is deliberately NOT
    consulted: a session the user registered is monitored whether or not steering
    happens to be on for it, and gating discovery on it left live asks unswept.

    Pass 1 (each root's own directory) is uncapped in practice, so a tight
    PERM_DIR_CAP degrades by dropping sub-agent directories first (R3) -- but
    only while pass 1 fits: 2 slots per distinct root directory, so >64 distinct
    root directories breaches the 128 cap (46 keys today => <=92).

    `con` is a shared read-only handle from the caller; None means "open one here
    and close it before returning", so a sweep passes its own single handle down
    instead of paying connect_db's retry storm twice per cycle. Note the
    asymmetry with perm_session_allowed, on purpose: here None means "get one
    myself" because discovery is best-effort and a DB-less directory list is
    still useful, whereas there None means "nothing is provable" and the grant
    decision must fail closed."""
    dirs = []

    def _add(d):
        # "." is what os.path.normpath("") returns, and an empty/NULL
        # `directory` must not smuggle it into the swept set (a bogus
        # "GET /permission?directory=." every 15s).
        d = d.strip() if isinstance(d, str) else ""
        if d and d not in ("/", "\\", ".") and d not in dirs:
            dirs.append(d)

    def _add_dir(d):
        """One directory in both observed spellings: the DB stores '/', the
        live route uses that form, asks record '\\'."""
        d = d.strip() if isinstance(d, str) else ""
        if not d:
            return
        _add(d)
        _add(os.path.normpath(d))

    for p in _perm_json(base, creds, "/project") or []:
        if isinstance(p, dict):
            _add(p.get("worktree"))
    _add(cfg.get("project_dir"))
    ms_all = cfg.get("monitored_sessions") or {}
    if not isinstance(ms_all, dict):
        ms_all = {}
    for ms in ms_all.values():
        if isinstance(ms, dict):
            _add(ms.get("project_dir"))
    roots = [sid for sid, ms in ms_all.items() if isinstance(ms, dict)]
    own_con = con is None
    con = _open_session_db() if own_con else con
    try:
        if con is not None:
            # Pass 1: one PK seek per monitored root. Cannot be starved by the
            # cap, and a sub-agent normally shares its root's directory -- so
            # this alone already covers the primary case.
            for sid in roots:
                _add_dir((session_row(con, sid) or {}).get("directory"))
            # Pass 2: bounded walk DOWN parent_id, for a sub-agent that works
            # somewhere else. active_leaf's proven per-node seek against
            # session.parent_id, kept as a bounded BFS rather than a recursive
            # CTE so the node budget can be enforced here.
            seen, queue, budget = set(roots), list(roots), PERM_SUBDIR_NODES
            while queue and budget > 0:
                for cid, cdir in con.execute(
                        "SELECT id, directory FROM session WHERE parent_id=? "
                        "ORDER BY time_updated DESC LIMIT ?",
                        (queue.pop(0), PERM_SUBDIR_FANOUT)).fetchall():
                    if cid in seen:
                        continue  # a mislinked row can name a node twice
                    seen.add(cid)
                    _add_dir(cdir)
                    budget -= 1
                    if budget > 0:
                        queue.append(cid)
            if budget <= 0:
                log_debug("permission_directories: lineage node budget %d hit"
                          % PERM_SUBDIR_NODES)
    except Exception as e:
        log_debug("permission_directories: %s" % e)
    finally:
        if own_con and con is not None:
            try:
                con.close()
            except Exception:
                pass
    if len(dirs) > PERM_DIR_CAP:
        log_debug("permission_directories: %d dirs, capped at %d"
                  % (len(dirs), PERM_DIR_CAP))
        dirs = dirs[:PERM_DIR_CAP]
    return dirs


def desktop_permission_sweep(cfg, base=None, creds=None) -> int:
    """Reply "always" to every pending external_directory permission request
    raised by a monitored session on the Desktop sidecar (the same call the GUI
    "accept" button makes).

    The sidecar serves two permission APIs and the GUI's file-location asks can
    land on either, so both are swept per project directory:

      v1  GET  /permission?directory=<dir>          -> [ {id, sessionID, permission, patterns} ]
          POST /permission/<rid>/reply?directory=<dir>
      v2  GET  /api/permission/request?location[directory]=<dir>
                                                    -> {"data": [ {id, sessionID, action, resources} ]}
          POST /api/session/<sid>/permission/<rid>/reply

    Returns how many requests were answered.

    Scope: an ask is answered only when its `sessionID` resolves through the DB
    parent/child lineage to a `monitored_sessions` root whose `auto_accept` is
    on (`enabled` is never consulted, and a session the user never registered is
    left hanging on purpose). Both the v1 and the v2 loop funnel through
    `_claim`, so that decision cannot drift between them."""
    if not cfg.get("auto_accept_external_dirs", True):
        return 0
    if base is None or creds is None:
        found = desktop_sidecar()
        if not found:
            return 0
        base, creds = found
    replied = 0
    now = time.time()
    # One best-effort read-only handle for the whole sweep, shared with
    # permission_directories, so neither a per-ask lineage walk nor discovery
    # pays connect_db's 5-retry 0.5*(i+1)s backoff of its own. Sharing it is
    # what makes con=None LOAD-BEARING: it is a real state, not a hypothetical,
    # because a missing/locked DB yields None and every ask then has to fail
    # closed -- so perm_session_allowed treats it as a hard reject, and never
    # opens a connection behind the sweep's back (R5, AC5(i)).
    con = _open_session_db()
    memo = {}  # ask sessionID -> allowed; both polarities, for this sweep only

    def _claim(req) -> str | None:
        """Request id, if this is an ask we auto-accept and have not just answered.

        Reads the sweep's own `con` from the enclosing scope -- both call sites
        passed that same object, and desktop_question_sweep's _claim below has
        the parameterless shape."""
        if _perm_action(req) not in PERM_AUTO_ACTIONS:
            return None  # cheapest test first; a bash ask opens no DB
        rid = req.get("id")
        if not rid:
            return None  # malformed ask; no DB
        sid = req.get("sessionID") or ""
        ok = memo.get(sid)
        if ok is None:
            ok = perm_session_allowed(cfg, sid, con)
            memo[sid] = ok
        if not ok:
            # Ahead of the dedupe write on purpose: a rejected ask must NOT
            # start a 15s timer, or ticking the dashboard checkbox would make
            # the grant wait one out (AC8).
            return None
        if _PERM_REPLIED.get(rid, 0.0) > now - 15:
            return None
        _PERM_REPLIED[rid] = now
        return rid

    def _accept(path, rid, ver, req, resources):
        st, raw = _perm_request(base, creds, path, method="POST", body={"reply": "always"})
        if st not in (200, 201, 202, 204):
            log_debug("permission reply %s -> HTTP %s %s" % (rid, st, raw[:120]))
            return 0
        target = (req.get("metadata") or {}).get("filepath") or ",".join(resources or [])
        log("permission auto-accept: %s %s session=%s resources=%s" %
            (rid, ver, req.get("sessionID"), target))
        record_permission_accept(rid, ver, req.get("sessionID"), target)
        return 1

    try:
        for d in permission_directories(cfg, base, creds, con):
            q = urllib.parse.quote(d, safe="")
            for req in _perm_json(base, creds, "/permission?directory=" + q) or []:
                rid = _claim(req)
                if rid:
                    replied += _accept("/permission/%s/reply?directory=%s" % (rid, q),
                                       rid, "v1", req, req.get("patterns"))
            body = _perm_json(base, creds, "/api/permission/request?location%5Bdirectory%5D=" + q)
            for req in (body.get("data") if isinstance(body, dict) else None) or []:
                rid = _claim(req)
                sid = req.get("sessionID")
                if rid and sid:
                    replied += _accept("/api/session/%s/permission/%s/reply" % (sid, rid),
                                       rid, "v2", req, req.get("resources"))
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    return replied


# ── Desktop question auto-answer ─────────────────────────────────────────────
_QUESTION_REPLIED = {}  # requestID -> timestamp of last reply (dedupe window 15s)


def _pick_answer(question: dict) -> list[str] | None:
    """Labels to auto-select for one question: the option whose label carries
    '(Recommended)' (case-insensitive), else the first option. None when the
    question has no options (free-text asks are left for a human)."""
    opts = [o for o in (question.get("options") or []) if isinstance(o, dict)]
    if not opts:
        return None
    for o in opts:
        if "recommended" in (o.get("label") or "").lower():
            return [o.get("label")]
    return [opts[0].get("label")]


def desktop_question_sweep(cfg, base=None, creds=None) -> int:
    """Answer pending question-tool requests on the Desktop sidecar with the
    recommended option (label containing 'Recommended', else the first option)
    -- the same call the GUI option buttons make.

    A `question` tool call blocks the worker's turn until answered, so an
    unanswered ask pins the session in 'busy' forever (the permission sweep's
    blind spot: self-check injections cannot unblock it either). The pending
    list routes are directory-scoped, so sweep per project directory:

      v1  GET  /question?directory=<dir>              -> [ {id, sessionID, questions, tool} ]
          POST /question/<rid>/reply?directory=<dir>  {"answers": [[labels]]}
      v2  GET  /api/question/request?location[directory]=<dir>
                                                     -> {"data": [...]}
          POST /api/session/<sid>/question/<rid>/reply

    'answers' is one entry per question, each an array of selected labels.
    Returns how many requests were answered."""
    if not cfg.get("auto_answer_questions", True):
        return 0
    if base is None or creds is None:
        found = desktop_sidecar()
        if not found:
            return 0
        base, creds = found
    answered = 0
    now = time.time()

    def _claim(req) -> str | None:
        """Request id, if not just answered (dedupe window 15s)."""
        rid = req.get("id") if isinstance(req, dict) else None
        if not rid or _QUESTION_REPLIED.get(rid, 0.0) > now - 15:
            return None
        _QUESTION_REPLIED[rid] = now
        return rid

    def _answers(req):
        out = []
        for q in req.get("questions") or []:
            labels = _pick_answer(q)
            if labels is None:
                return None
            out.append(labels)
        return out

    def _reply(path, rid, ver, req):
        body = {"answers": _answers(req)}
        if body["answers"] is None:
            log_debug("question %s has an option-less question; left for a human" % rid)
            return 0
        st, raw = _perm_request(base, creds, path, method="POST", body=body)
        if st not in (200, 201, 202, 204):
            log_debug("question reply %s -> HTTP %s %s"
                      % (rid, st, raw[:120].decode("utf-8", "replace")
                         if isinstance(raw, bytes) else str(raw)[:120]))
            return 0
        log("question auto-answer: %s %s session=%s answers=%s"
            % (rid, ver, req.get("sessionID"),
               json.dumps(body["answers"], ensure_ascii=False)))
        record_permission_accept(rid, ver, req.get("sessionID"),
                                 json.dumps(body["answers"], ensure_ascii=False),
                                 kind="question_answer")
        return 1

    for d in permission_directories(cfg, base, creds):
        q = urllib.parse.quote(d, safe="")
        for req in _perm_json(base, creds, "/question?directory=" + q) or []:
            rid = _claim(req)
            if rid:
                answered += _reply("/question/%s/reply?directory=%s" % (rid, q),
                                   rid, "v1", req)
        body = _perm_json(base, creds, "/api/question/request?location%5Bdirectory%5D=" + q)
        for req in (body.get("data") if isinstance(body, dict) else None) or []:
            rid = _claim(req)
            sid = req.get("sessionID")
            if rid and sid:
                answered += _reply("/api/session/%s/question/%s/reply" % (sid, rid),
                                   rid, "v2", req)
    return answered


def permission_event_watcher(stop: threading.Event):
    """Watch the Desktop /event stream and the pending-permission lists.

    Answers external_directory asks immediately on SSE "permission" events
    (v1 emits permission.asked, v2 permission.v2.asked) AND on a ~15s backstop
    sweep, and answers pending question-tool asks the same way (a `question` tool
    call blocks the worker turn until answered). The backstop runs on its own
    thread because the SSE read blocks: a quiet stream would otherwise pin the
    sweep to the socket timeout.
    Re-discovers port + credentials when the Desktop restarts."""
    import base64
    import urllib.request
    cfg_holder = {"cfg": {}}  # load once per sweep instead of per line
    sweep_lock = threading.Lock()
    while not stop.is_set():
        found = desktop_sidecar()
        if not found:
            if stop.wait(10):
                return
            continue
        base, creds = found
        auth = "Basic " + base64.b64encode(
            ("%s:%s" % (creds["username"], creds["password"])).encode("utf-8")).decode("ascii")
        stream_done = threading.Event()

        def _sweep_now():
            with sweep_lock:
                try:
                    cfg_holder["cfg"] = load_config()
                    desktop_permission_sweep(cfg_holder["cfg"], base=base, creds=creds)
                    desktop_question_sweep(cfg_holder["cfg"], base=base, creds=creds)
                except Exception as e:
                    log_debug("permission sweep error: %s" % e)

        def _backstop():
            while not (stop.is_set() or stream_done.is_set()):
                _sweep_now()
                if stream_done.wait(15):
                    return

        backstop = threading.Thread(target=_backstop, name="perm-sweep", daemon=True)
        backstop.start()
        try:
            req = urllib.request.Request(base + "/event", headers={"Authorization": auth})
            with urllib.request.urlopen(req, timeout=55) as r:
                log("permission watcher connected: %s/event" % base)
                while not stop.is_set():
                    line = r.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", "replace").strip()
                    if not text.startswith("data:"):
                        continue
                    try:
                        ev = json.loads(text[5:].strip())
                    except Exception:
                        continue
                    etype = str(ev.get("type", "")).lower()
                    if "permission" in etype or "question" in etype:
                        _sweep_now()
        except Exception as e:
            log_debug("permission watcher dropped: %s" % e)
        finally:
            stream_done.set()
            backstop.join(timeout=5)
        stop.wait(5)


# ── llama.cpp activity probe ─────────────────────────────────────────────────
class ProcessTimes:
    """Thin ctypes wrapper to read process CPU + wall time on Windows (stdlib)."""

    def __init__(self):
        import ctypes
        from ctypes import wintypes
        self.ctypes = ctypes
        self.wintypes = wintypes

        class FILETIME(ctypes.Structure):
            _fields_ = [("dwLowDateTime", wintypes.DWORD),
                        ("dwHighDateTime", wintypes.DWORD)]

        self.FILETIME = FILETIME
        k32 = ctypes.windll.kernel32
        OpenProcess = k32.OpenProcess
        OpenProcess.restype = wintypes.HANDLE
        OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        self.OpenProcess = OpenProcess
        GetProcessTimes = k32.GetProcessTimes
        GetProcessTimes.argtypes = [wintypes.HANDLE,
                                    ctypes.POINTER(FILETIME), ctypes.POINTER(FILETIME),
                                    ctypes.POINTER(FILETIME), ctypes.POINTER(FILETIME)]
        GetProcessTimes.restype = wintypes.BOOL
        self.GetProcessTimes = GetProcessTimes
        GetSystemTimeAsFileTime = k32.GetSystemTimeAsFileTime
        GetSystemTimeAsFileTime.argtypes = [ctypes.POINTER(FILETIME)]
        self.GetSystemTimeAsFileTime = GetSystemTimeAsFileTime

    def _ms(self, ft) -> int:
        return (getattr(ft, "dwHighDateTime", 0) << 32
                | getattr(ft, "dwLowDateTime", 0)) // 10000

    def sample(self, pid: int):
        """Return (wall_ms, cpu_ms) for the given PID, or None if unavailable."""
        h = self.OpenProcess(0x0400, False, int(pid))
        if not h:
            return None
        try:
            c = self.FILETIME(); e = self.FILETIME()
            k = self.FILETIME(); u = self.FILETIME(); t = self.FILETIME()
            if not self.GetProcessTimes(h, self.ctypes.byref(c), self.ctypes.byref(e),
                                        self.ctypes.byref(k), self.ctypes.byref(u)):
                return None
            self.GetSystemTimeAsFileTime(self.ctypes.byref(t))
            return (self._ms(t), self._ms(k) + self._ms(u))
        finally:
            try:
                self.ctypes.windll.kernel32.CloseHandle(h)
            except Exception:
                pass


_PROC_TIMES = None  # lazy singleton


class ProcessMemIo:
    """ctypes wrapper for process working-set (RAM) and disk IO counters."""

    def __init__(self):
        import ctypes
        from ctypes import wintypes
        self.ctypes = ctypes

        class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD),
                        ("PageFaultCount", wintypes.DWORD),
                        ("PeakWorkingSetSize", ctypes.c_size_t),
                        ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t),
                        ("PeakPagefileUsage", ctypes.c_size_t)]

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [("ReadOperationCount", ctypes.c_uint64),
                        ("WriteOperationCount", ctypes.c_uint64),
                        ("OtherOperationCount", ctypes.c_uint64),
                        ("ReadTransferCount", ctypes.c_uint64),
                        ("WriteTransferCount", ctypes.c_uint64),
                        ("OtherTransferCount", ctypes.c_uint64)]

        self.PMC = PROCESS_MEMORY_COUNTERS
        self.IOC = IO_COUNTERS
        k32 = ctypes.windll.kernel32
        self.OpenProcess = k32.OpenProcess
        self.OpenProcess.restype = wintypes.HANDLE
        self.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        self.GetProcessMemoryInfo = ctypes.windll.psapi.GetProcessMemoryInfo
        self.GetProcessIoCounters = k32.GetProcessIoCounters
        self.CloseHandle = k32.CloseHandle

    def sample(self, pid: int):
        """Return (working_set_bytes, io_read_bytes, io_write_bytes) or None."""
        h = self.OpenProcess(0x0400, False, int(pid))  # PROCESS_QUERY_INFORMATION
        if not h:
            return None
        try:
            pmc = self.PMC()
            pmc.cb = self.ctypes.sizeof(self.PMC)
            if not self.GetProcessMemoryInfo(h, self.ctypes.byref(pmc), pmc.cb):
                return None
            ioc = self.IOC()
            if not self.GetProcessIoCounters(h, self.ctypes.byref(ioc)):
                return (int(pmc.WorkingSetSize), 0, 0)
            return (int(pmc.WorkingSetSize),
                    int(ioc.ReadTransferCount), int(ioc.WriteTransferCount))
        finally:
            try:
                self.CloseHandle(h)
            except Exception:
                pass


_PROC_MEMIO = None  # lazy singleton


def llama_mem_io(pid: int | None):
    """Working set + disk IO counters for the llama process (stdlib ctypes)."""
    if not pid:
        return None
    global _PROC_MEMIO
    if _PROC_MEMIO is None:
        _PROC_MEMIO = ProcessMemIo()
    try:
        return _PROC_MEMIO.sample(pid)
    except Exception as e:
        log_debug("llama_mem_io: %s" % e)
        return None


def llama_api_base(cfg) -> str:
    port = int(cfg.get("llama_api_port", 1234))
    host = str(cfg.get("llama_api_host", "127.0.0.1"))
    return "http://%s:%d" % (host, port)


def llama_slots(cfg):
    """Query llama-server /slots for the live KV-cache/context state, or None.
    Sample fields: n_ctx, n_prompt_tokens, n_prompt_tokens_cache,
    n_prompt_tokens_processed, is_processing, id_task."""
    import urllib.request
    try:
        with urllib.request.urlopen(llama_api_base(cfg) + "/slots", timeout=2.0) as r:
            d = json.loads(r.read().decode("utf-8", "replace"))
        if isinstance(d, list) and d:
            s = d[0]
            nt = s.get("next_token")
            if isinstance(nt, list):
                nt = nt[0] if nt else {}
            if not isinstance(nt, dict):
                nt = {}
            return {
                "n_ctx": int(s.get("n_ctx") or 0),
                "prompt_tokens": int(s.get("n_prompt_tokens") or 0),
                "cache_tokens": int(s.get("n_prompt_tokens_cache") or 0),
                "processed_tokens": int(s.get("n_prompt_tokens_processed") or 0),
                "decoded_tokens": int(nt.get("n_decoded") or 0),
                "is_processing": bool(s.get("is_processing")),
                "id_task": s.get("id_task"),
            }
    except Exception:
        return None
    return None


def llama_pid(cfg) -> int | None:
    """Return the llama-server process id (config override, else auto-detect by
    image name), or None if not found."""
    pid = int(cfg.get("llama_pid") or 0)
    if pid:
        return pid
    try:
        out = subprocess.run(["tasklist", "/FI", "IMAGENAME eq llama-server.exe",
                              "/FO", "CSV", "/NH"],
                             capture_output=True, text=True, encoding="utf-8",
                             errors="replace", timeout=15,
                             creationflags=creation_flags()).stdout
        for m in re.finditer(r'"([^"]+)","(\d+)"', out):
            if "llama" in m.group(1).lower():
                return int(m.group(2))
    except Exception as e:
        log_debug("llama_pid: %s" % e)
    return None


def llama_server_alive(cfg) -> bool:
    """True when the llama-server process exists. Uses llama_pid() which checks
    tasklist; returns False when no llama-server.exe is found. This is the
    liveness watchdog signal: when False, the server has died and the monitor
    must trigger a restart immediately, bypassing all cooldown/cadence logic."""
    return llama_pid(cfg) is not None


def llama_respawn(cfg, timeout_s=None) -> bool:
    """Respawn llama-server via its bat file and wait for /health to return 200.
    Called by the watchdog when the server is detected dead. Returns True when
    the server is back and healthy, False on timeout or launch failure.

    Capture discipline: stdout/stderr go to the canonical console log
    (llama-server.out.log, same target llama_restart._respawn uses), never
    DEVNULL, so the dashboard tailer can see the new era. While a server is
    already present this is a no-op that reports True - the liveness watchdog
    and the monitor supervisor both run this path and must never double-
    spawn an extra instance."""
    if llama_server_alive(cfg):
        log("LLAMA-DOWN: server already alive (pid %s); skipping respawn"
            % llama_pid(cfg))
        return True
    bat = cfg.get("llama_bat_path", r"E:\QWen3.8Opencode-VISION-TURBO.bat")
    log("LLAMA-DOWN: respawning via %s" % bat)
    try:
        import llama_restart as _lr
        log_fn = _lr.LOG_FN
    except Exception:
        log_fn = "llama-server.out.log"
    log_path = home_dir() / log_fn
    try:
        fh = open(log_path, "ab")
    except Exception:
        fh = None
    try:
        proc = subprocess.Popen(
            ["cmd", "/c", bat],
            stdout=fh, stderr=subprocess.STDOUT,
            creationflags=creation_flags(),
        )
    except Exception as e:
        log("LLAMA-DOWN: respawn launch failed: %s" % e)
        return False
    finally:
        if fh:
            try:
                fh.close()
            except Exception:
                pass
    # Wait for health (model load takes time; use the same timeout as restart).
    if timeout_s is None:
        timeout_s = int(cfg.get("llama_restart_health_timeout_s", 240))
    base = llama_api_base(cfg)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(base + "/health", timeout=3) as r:
                if r.status == 200:
                    log("LLAMA-DOWN: server back up and healthy (pid %s, "
                        "%ds)" % (llama_pid(cfg), int(time.time() - (deadline - timeout_s))))
                    return True
        except Exception:
            pass
        time.sleep(3.0)
    log("LLAMA-DOWN: respawn timed out after %ds; server may need manual attention"
        % timeout_s)
    return False


def llama_cpu_frac(cfg, state, agent_window_s: int | None = None) -> float:
    """Return an estimate of llama-server CPU activity over the recent window:
    cpu_fraction = llama CPU-seconds / wall-seconds in the window. 0.0 means
    idle/unavailable. Persists the last sample in state for cross-poll deltas.

    We sample now and compare against the previous sample + its wall timestamp;
    the effective window is (now - previous wall time), clamped to
    stall_llama_window_s so we look back at most that far."""
    global _PROC_TIMES
    pid = llama_pid(cfg)
    if not pid:
        return 0.0
    if _PROC_TIMES is None:
        _PROC_TIMES = ProcessTimes()
    now = int(time.time() * 1000)
    samp = _PROC_TIMES.sample(pid)
    if not samp:
        return 0.0
    wall_now, cpu_now = samp
    prev = state.get("_llama")
    maxback = int(cfg.get("stall_llama_window_s", 120)) * 1000
    if isinstance(prev, dict) and prev.get("cpu") is not None:
        d_cpu = max(0, cpu_now - int(prev["cpu"]))
        d_wall = now - int(prev["wall"])
        if 0 < d_wall <= maxback * 1.5:
            frac = d_cpu / d_wall
        else:
            # Too long since last sample: cannot attribute old CPU to now. Use a
            # short self-sample over agent_window_s if provided, else treat the
            # fresh sample as the baseline (frac 0 until the next sample).
            if agent_window_s:
                time.sleep(min(agent_window_s, 5))
                samp2 = _PROC_TIMES.sample(pid)
                if samp2:
                    wall_now, cpu_now = samp2
                    d_cpu = max(0, cpu_now - samp[1])
                    d_wall = wall_now - samp[0]
                    frac = d_cpu / d_wall if d_wall > 0 else 0.0
                else:
                    frac = 0.0
            else:
                frac = 0.0
    else:
        frac = 0.0
    state["_llama"] = {"wall": now, "cpu": cpu_now}
    return max(0.0, min(frac, 8.0))


def llama_slots_processing(cfg) -> bool:
    """True if llama-server reports slot 0 actively processing (prompt/decode).
    False when the server is unreachable (treated as 'no signal', not 'active')."""
    try:
        slots = llama_slots(cfg)
        return bool(slots and slots.get("is_processing"))
    except Exception as e:
        log_debug("llama_slots_processing: %s" % e)
        return False


def llama_token_speeds(cfg, gap_s=0.5):
    """Sample llama-server /slots twice a short gap apart and return
    (output_tps, ingest_tps) — the rate at which tokens are currently being
    decoded vs prompt-processed. (0.0, 0.0) when unreachable. A positive
    output speed means the model is mid-generation even if the server didn't
    flag is_processing on this build."""
    s1 = llama_slots(cfg)
    if not s1:
        return 0.0, 0.0
    t0 = time.monotonic()
    time.sleep(gap_s)
    s2 = llama_slots(cfg)
    if not s2:
        return 0.0, 0.0
    dt = time.monotonic() - t0
    if dt <= 0 or not s1.get("id_task") or not s2.get("id_task") \
            or s1.get("id_task") != s2.get("id_task"):
        # Task boundary (new task started between samples) or no active task:
        # treat as untrusted — the caller decides how to treat this.
        return 0.0, 0.0
    out = max(0.0, (s2.get("decoded_tokens", 0) - s1.get("decoded_tokens", 0))) / dt
    ingest = max(0.0, (s2.get("processed_tokens", 0) - s1.get("processed_tokens", 0))) / dt
    return out, ingest


# GPU engine utilisation, the same universal, no-3rd-party method Task Manager
# uses: PDH '\GPU Engine(*)\Utilization Percentage'. We read the LARGEST single
# engine value across ALL processes — the per-pid filter read 0 on this AMD
# driver mid-generation, but the all-engine max reports the real load (~90-95%
# while llama generates, <10% idle). This is the single source of truth for the
# GPU read: both the dashboard telemetry loop and llama_gpu_util() call
# gpu_engine_max() below.
#
# Ported 1:1 from the working telemetry_monitor_fast.ahk: one persistent PDH
# query (opened once) whose PdhCollectQueryData is called once per read. Two
# PDH quirks the AHK relies on — and we must keep:
#   * the size query (pItems=NULL) returns a NON-ZERO status yet still fills the
#     buffer-size/count out-params, so we must NOT bail on that return code;
#   * the fill's 6th param (pdwLength) is passed 0 by value (PDH sizes from #3).
_GPU_SYS_COUNTER = r"\GPU Engine(*)\Utilization Percentage"
_GPU_MEM_COUNTER = r"\GPU Adapter Memory(*)\Dedicated Usage"
_DISK_IDLE_COUNTER = r"\PhysicalDisk(*)\% Idle Time"
_PDH_GPU = {"ok": False, "err": None, "q": None, "c": None, "mc": None, "dc": None}
_PDH_GPU_LOCK = threading.Lock()


def _pdh_gpu_init() -> bool:
    """Open the persistent all-engine GPU counter query once (returns ok)."""
    if _PDH_GPU["ok"] or _PDH_GPU["err"] is not None:
        return _PDH_GPU["ok"]
    if os.name != "nt":
        _PDH_GPU["err"] = "not windows"
        return False
    try:
        import ctypes as C
        pdh = C.windll.pdh
        pdh.PdhOpenQueryW.argtypes = [C.c_void_p, C.c_void_p,
                                      C.POINTER(C.c_void_p)]
        pdh.PdhOpenQueryW.restype = C.c_long
        pdh.PdhAddEnglishCounterW.argtypes = [
            C.c_void_p, C.c_wchar_p, C.c_ulong, C.POINTER(C.c_void_p)]
        pdh.PdhAddEnglishCounterW.restype = C.c_long
        pdh.PdhCollectQueryData.argtypes = [C.c_void_p]
        pdh.PdhCollectQueryData.restype = C.c_long
        pdh.PdhGetFormattedCounterArrayW.argtypes = [
            C.c_void_p, C.c_ulong, C.POINTER(C.c_ulong),
            C.POINTER(C.c_ulong), C.c_void_p, C.c_ulong]
        pdh.PdhGetFormattedCounterArrayW.restype = C.c_long
        q = C.c_void_p()
        if pdh.PdhOpenQueryW(None, None, C.byref(q)) != 0:
            _PDH_GPU["err"] = "PdhOpenQueryW"
            return False
        c = C.c_void_p()
        if pdh.PdhAddEnglishCounterW(q, _GPU_SYS_COUNTER, 0,
                                     C.byref(c)) != 0:
            pdh.PdhCloseQuery(q)
            _PDH_GPU["err"] = "PdhAddEnglishCounterW"
            return False
        mc = C.c_void_p()
        if pdh.PdhAddEnglishCounterW(q, _GPU_MEM_COUNTER, 0,
                                     C.byref(mc)) != 0:
            mc = None  # adapter-memory counter absent -> engine-only mode
        dc = C.c_void_p()
        if pdh.PdhAddEnglishCounterW(q, _DISK_IDLE_COUNTER, 0,
                                     C.byref(dc)) != 0:
            dc = None  # disk counter absent (rare) -> no disk activity feed
        _PDH_GPU.update(q=q, c=c, mc=mc, dc=dc, ok=True)
        pdh.PdhCollectQueryData(q)  # prime; first sample is 0
        return True
    except Exception as e:
        _PDH_GPU["err"] = str(e)
        log_debug("pdh gpu init: %s" % e)
        return False


def _arr_vals(h) -> list:
    """Per-instance (name, value) list for a (*)-style PDH counter. Returns []
    when the counter is unavailable or the array cannot be read. Items are
    PDH_FMT_COUNTERVALUE_ITEM: PWSTR szName (offset 0) + PDH_FMT_COUNTERVALUE
    (CStatus DWORD at offset 8, value double at offset 16). Rows whose CStatus
    is invalid (first sample or a transient fail) are dropped, never treated
    as real data."""
    if not h:
        return []
    import ctypes as C
    pdh = C.windll.pdh
    sz = C.c_ulong()
    cnt = C.c_ulong()
    pdh.PdhGetFormattedCounterArrayW(h, 0x200, C.byref(sz), C.byref(cnt),
                                     None, 0)
    if cnt.value == 0 or sz.value < 24:
        return []
    buf = (C.c_ubyte * sz.value)()
    if pdh.PdhGetFormattedCounterArrayW(h, 0x200, C.byref(sz), C.byref(cnt),
                                        buf, 0) != 0:
        return []
    base = C.addressof(buf)
    out = []
    for i in range(cnt.value):
        item = base + i * 24
        try:
            status = C.c_ulong.from_address(item + 8).value
            if status > 1:  # PDH_CSTATUS_VALID_DATA(0)/NEW_DATA(1) only
                continue
            name_ptr = C.c_longlong.from_address(item).value
            name = C.wstring_at(name_ptr) if name_ptr else ""
        except Exception:
            continue
        v = C.c_double.from_address(item + 16).value
        out.append((name, v))
    return out


def _arr_max(h):
    """Highest (name, value) value for a (*)-style counter, or None."""
    vals = [v for _, v in _arr_vals(h)]
    return max(vals) if vals else None


def _gpu_group(rows, agg_highest):
    """Group per-engine/-segment PDH rows by adapter LUID.

    WDDM instance names carry the adapter identity as a `luid_<H>_<L>_phys_<N>`
    token run: engine rows are per-PROCESS (`pid_1234_luid_0x…_0x…_phys_0_
    eng_0_engtype_3D`) while adapter-memory rows start with it directly
    (`luid_0x…_0x…_phys_0`). The LUID run is located by finding the `luid`
    token and taking it plus the next four. Rows without a LUID are dropped
    (e.g. a hypothetical _Total). Returns [(luid, value)] in first-seen (WDDM
    enumeration) order: when agg_highest is true the adapter value is the max
    of its engines (a GPU's processing % is its busiest engine — and with
    per-process rows, across all processes sharing the adapter), otherwise the
    sum (total VRAM usage across phys segments)."""
    groups: dict = {}
    order: list = []
    for name, v in rows:
        parts = name.split("_")
        try:
            i = parts.index("luid")
        except ValueError:
            continue
        if i + 5 > len(parts):
            continue
        key = "_".join(parts[i:i + 5])
        if key not in groups:
            groups[key] = 0.0
            order.append(key)
        groups[key] = max(groups[key], v) if agg_highest else groups[key] + v
    return [(k, groups[k]) for k in order]


def _pdh_gpu_read() -> tuple:
    """One fresh collect on the persistent query.

    Returns (max_engine_pct, vram_mb, gpu_mem, gpu_eng, disk_inst): the
    highest single GPU-engine utilisation (0-100, all adapters), the system
    total dedicated GPU memory in use (MB, sum across adapters), per-adapter
    (LUID) dedicated usage [(luid, MB)] and per-adapter engine utilisation
    [(luid, pct)] both in WDDM enumeration order, and the per-physical-disk
    idle % [(instance, idle_pct)]. Empty lists when a counter is absent."""
    if not _pdh_gpu_init():
        return 0.0, None, [], [], []
    import ctypes as C
    pdh = C.windll.pdh
    q, c, mc = _PDH_GPU["q"], _PDH_GPU["c"], _PDH_GPU["mc"]
    pdh.PdhCollectQueryData(q)

    engine = _arr_max(c) or 0.0
    gpu_eng = [(k, round(v, 1)) for k, v in _gpu_group(_arr_vals(c), True)]
    vram_mb = None
    gpu_mem = []
    if mc:
        rows = _arr_vals(mc)
        b = _arr_max(mc)
        if b is not None:
            # GPU Adapter Memory "Dedicated Usage" is reported in bytes
            vram_mb = round(b / (1024.0 * 1024.0), 1)
        gpu_mem = [(k, round(v / (1024.0 * 1024.0), 1))
                   for k, v in _gpu_group(rows, False)]
    disk_inst = []
    if _PDH_GPU["dc"]:
        disk_inst = _arr_vals(_PDH_GPU["dc"])  # % Idle Time per physical disk
    return engine, vram_mb, gpu_mem, gpu_eng, disk_inst


def gpu_engine_max(state=None) -> float:
    """Highest single GPU-engine utilisation (%) across all processes, or 0.

    Read straight from PDH (persistent query, no PowerShell). PDH collects are
    performed by one background sampler thread so a stalled PdhCollectQueryData
    (observed under concurrent llama model loads) can never freeze the monitor
    poll; callers always get the latest completed sample (~1s fresh)."""
    _ensure_gpu_sampler()
    return _PDH_SAMPLE.get("val", 0.0)


_PDH_SAMPLE = {"val": 0.0, "vram_mb": None, "gpu_inst": [], "gpu_eng": [],
               "disk_inst": [], "t": 0.0}


def gpu_adapters_usage_mb() -> list:
    """Per-adapter dedicated GPU VRAM in use (MB): [(luid, mb)] in WDDM
    enumeration order — the same order the dashboard matches to its
    registry-derived adapter totals. [] when the counter is unavailable."""
    if os.name != "nt":
        return []
    _ensure_gpu_sampler()
    return list(_PDH_SAMPLE.get("gpu_inst") or [])


def gpu_adapters_engine_pct() -> list:
    """Per-adapter GPU PROCESSING utilisation %: [(luid, pct)] where pct is
    the adapter's busiest engine (3D/Copy/VideoEncode/VideoDecode) — the same
    semantics as the all-engine gauge but scoped per adapter. Same LUID order
    as gpu_adapters_usage_mb(). [] when the counter is unavailable."""
    if os.name != "nt":
        return []
    _ensure_gpu_sampler()
    return list(_PDH_SAMPLE.get("gpu_eng") or [])


def physical_disk_active_pct() -> list:
    """Real-time disk activity % per physical disk: [(instance, pct)] where
    pct = 100 - %Idle Time (Task-Manager "Active time" semantics). Instances
    are plain disk indexes (e.g. "0", "1"); the _Total row is dropped. [] when
    the disk counter is absent (non-Windows / rare failure)."""
    if os.name != "nt":
        return []
    _ensure_gpu_sampler()
    out = []
    for name, idle in _PDH_SAMPLE.get("disk_inst") or []:
        if not name or name in ("_Total", "_"):
            continue
        try:
            int(name.split()[0])  # disk instances are numeric indexes
        except ValueError:
            continue
        out.append((name.split()[0], round(100.0 - float(idle), 1)))
    return out
_GPU_SAMPLER_LOCK = threading.Lock()
_GPU_SAMPLER_STARTED = False


def _ensure_gpu_sampler():
    """Lazily start the one PDH sampling thread that owns the GPU query."""
    global _GPU_SAMPLER_STARTED
    if _GPU_SAMPLER_STARTED:
        return
    if os.name != "nt":
        return
    with _GPU_SAMPLER_LOCK:
        if _GPU_SAMPLER_STARTED:
            return
        _GPU_SAMPLER_STARTED = True
        threading.Thread(target=_gpu_sampler_loop, daemon=True,
                         name="gpu-sampler").start()


def _gpu_sampler_loop():
    while True:
        try:
            with _PDH_GPU_LOCK:
                v, vram, gpu_mem, gpu_eng, disk_inst = _pdh_gpu_read()
            _PDH_SAMPLE["val"] = round(float(v), 1)
            _PDH_SAMPLE["vram_mb"] = vram
            _PDH_SAMPLE["gpu_inst"] = gpu_mem
            _PDH_SAMPLE["gpu_eng"] = gpu_eng
            _PDH_SAMPLE["disk_inst"] = disk_inst
            _PDH_SAMPLE["t"] = time.monotonic()
        except Exception:
            pass  # keep sampling; a transient PDH stall must not end the thread
        time.sleep(1.0)


def gpu_vram_used_mb() -> float | None:
    """System-wide dedicated GPU VRAM in use (MB, max across adapters), or None.

    Backed by the same background PDH sampler as gpu_engine_max(): reads the
    `GPU Adapter Memory(*)\\Dedicated Usage` counter (~1s fresh). None when the
    counter is unavailable (non-Windows, engine-only PDH mode)."""
    if os.name != "nt":
        return None
    _ensure_gpu_sampler()
    return _PDH_SAMPLE.get("vram_mb")


def llama_gpu_util(cfg, state=None):
    r"""Return llama-server's GPU utilisation as {'sum': x, 'max': x} (percents),
    or {'sum':0,'max':0} when llama isn't running.

    Reads the SAME all-engine PDH max as the dashboard's gpu_system_max().
    Per-process GPU is NOT available on this box (AMD RX 9060 XT WDDM): the
    "GPU Process" object doesn't exist — verified 4 ways (every
    `\GPU Process(*)\…` PDH path is INVALID_PATH, `typeperf -q "GPU Process"`
    → not found, no Win32_PerfRawData_GPU* WMI classes, no Perflib registry
    object). The all-engine max is therefore NOT a faithful proxy for llama's
    own activity: desktop/compositor/other-app noise sits right at the old
    ~10% idle threshold (observed 10.7% with llama verifiably idle, 2026-09-22,
    which false-blocked self-checks for ~20 min). llama_idle() uses it as a
    fallback signal only — consulted when both direct gates (slots, token
    speeds) are disabled. We still gate on llama being present so an
    unrelated GPU consumer isn't counted as llama activity. Cached in state for
    llama_gpu_cache_s so llama_idle's gate doesn't re-collect every poll."""
    pid = llama_pid(cfg)
    if not pid:
        return {"sum": 0.0, "max": 0.0}
    now = time.monotonic()
    if state is not None:
        c = state.get("_gpu")
        if isinstance(c, dict) and c.get("pid") == pid and \
                (now - c.get("t", 0.0)) < float(cfg.get("llama_gpu_cache_s", 5.0)):
            return {"sum": c["sum"], "max": c["max"]}
    x = gpu_engine_max()
    res = {"sum": float(x), "max": float(x)}
    if state is not None:
        state["_gpu"] = {"t": now, "pid": pid,
                         "sum": res["sum"], "max": res["max"]}
    return res


# ── Memory totals (VRAM + system RAM) for the dashboard Y-axis pins ─────────
# These back the two "usage" charts' pinned Y-axis and the "used / total (%)"
# tile readout. WMI AdapterRAM is unreliable on this box (reports ~4 GiB for a
# 16 GB card), so the VRAM total comes from the gpu_vram_total_mb config
# (default 16384 == RX 9060 XT) with an opt-in, sanity-bounded DXGI auto-detect
# for when the user sets it to 0.
_GVRAM = {"mb": None}
_SRAM = {"mb": None}


def _dxgi_dedicated_vram_mb() -> float | None:
    """Best-effort dedicated GPU VRAM (MB) via DXGI adapter GetDesc.
    Returns None on ANY failure so the caller falls back to the config default.
    Opt-in only (used when gpu_vram_total_mb == 0); not on the default path."""
    try:
        import ctypes as C
        from ctypes import wintypes
        import uuid

        C.windll.dxgi.LoadLibrary()
        C.windll.dxgi.CreateDXGIFactory1.argtypes = [C.c_void_p, C.c_void_p]
        C.windll.dxgi.CreateDXGIFactory1.restype = C.c_long

        def refiid(hexstr):
            return (C.c_ubyte * 16).from_buffer_copy(
                uuid.UUID(hexstr).bytes_le)

        # IDXGIFactory1: {77da53c0-9989-407a-b0b6-c6999a8acaf3}
        iidf = refiid("77da53c0-9989-407a-b0b6-c6999a8acaf3")
        factory = C.c_void_p()
        if C.windll.dxgi.CreateDXGIFactory1(C.byref(iidf),
                                           C.byref(factory)) != 0 or not factory:
            return None
        def vtable(ptr):
            return C.cast(C.cast(ptr, C.c_void_p),
                          C.POINTER(C.c_void_p * 16))[0]

        # IDXGIFactory vtable: index 3 = GetAdapter(UINT, IDXGIAdapter**)
        get_adapter = C.cast(
            vtable(factory)[3],
            C.CFUNCTYPE(C.c_long, C.c_void_p, C.c_uint, C.POINTER(C.c_void_p)))
        adapter = C.c_void_p()
        if get_adapter(factory, 0, C.byref(adapter)) != 0 or not adapter:
            return None
        # IDXGIAdapter vtable: index 3 = GetDesc(OUT IDXGI_ADAPTER_DESC*)
        get_desc = C.cast(
            vtable(adapter)[3], C.CFUNCTYPE(C.c_long, C.c_void_p, C.c_void_p))
        # IDXGI_ADAPTER_DESC (x64): DedicatedVideoMemory (SIZE_T) is the second
        # SIZE_T after Description[128]/DeviceId/DeviceLuid/Flags/three UINTs/
        # DriverVersion. Read a wide window and sanity-check the value instead of
        # trusting an exact offset (opt-in path; wrong read -> None -> fallback).
        desc = (C.c_ubyte * 384)()
        if get_desc(adapter, desc) != 0:
            return None
        for off in range(280, 328, 8):
            dv = int.from_bytes(desc[off:off + 8], "little")
            mb = dv / (1024 * 1024)
            if 1024 <= mb <= 256 * 1024:  # 1 GiB .. 256 GiB is plausible
                return mb
        return None
    except Exception:
        return None


def gpu_vram_total_mb() -> float:
    """Total GPU VRAM (MB) for pinning the VRAM chart. The config override
    (default 16384) wins when > 0; when set to 0, try DXGI auto-detect; else the
    default. Cached — the total does not change at runtime."""
    if _GVRAM["mb"] is None:
        val = float(load_config().get("gpu_vram_total_mb", 16384) or 0)
        if val > 0:
            _GVRAM["mb"] = val
        else:
            auto = _dxgi_dedicated_vram_mb()
            _GVRAM["mb"] = float(auto) if auto is not None else 16384.0
    return float(_GVRAM["mb"])


def system_ram_total_mb() -> float:
    """Total physical RAM (MB) via GlobalMemoryStatusEx. 0 when unavailable."""
    if _SRAM["mb"] is None:
        mb = 0.0
        try:
            import ctypes as C
            from ctypes import wintypes

            # NOTE: this x64 build's GlobalMemoryStatusEx requires dwLength=64
            # (the natural ctypes layout of the 6 SIZE_T fields is 56 bytes), so a
            # trailing 8-byte pad is added to match and avoid the API writing past
            # the buffer. Field offsets (dwTotalPhys @ 8) were verified live.
            class MEMORYSTATUSEx(C.Structure):
                _fields_ = [("dwLength", wintypes.DWORD),
                            ("dwMemoryLoad", wintypes.DWORD),
                            ("dwTotalPhys", C.c_size_t),
                            ("dwAvailPhys", C.c_size_t),
                            ("dwTotalPageFile", C.c_size_t),
                            ("dwAvailPageFile", C.c_size_t),
                            ("dwTotalVirtual", C.c_size_t),
                            ("dwAvailVirtual", C.c_size_t),
                            ("_pad", C.c_uint64)]

            ms = MEMORYSTATUSEx()
            ms.dwLength = C.sizeof(MEMORYSTATUSEx)
            if C.windll.kernel32.GlobalMemoryStatusEx(C.byref(ms)):
                mb = int(ms.dwTotalPhys) / (1024 * 1024)
        except Exception:
            mb = 0.0
        _SRAM["mb"] = mb
    return float(_SRAM["mb"])


def system_ram_used_mb() -> float | None:
    """System-wide physical RAM in use (MB) via GlobalMemoryStatusEx.

    Matches Task Manager's "in use" (= total - available; available is the
    sum of the standby, free and zero lists). Fresh query every call: RAM use
    drifts, so unlike system_ram_total_mb() this is not cached. None when the
    API call fails."""
    try:
        import ctypes as C
        from ctypes import wintypes

        # same padded layout as system_ram_total_mb() (see note there)
        class MEMORYSTATUSEx(C.Structure):
            _fields_ = [("dwLength", wintypes.DWORD),
                        ("dwMemoryLoad", wintypes.DWORD),
                        ("dwTotalPhys", C.c_size_t),
                        ("dwAvailPhys", C.c_size_t),
                        ("dwTotalPageFile", C.c_size_t),
                        ("dwAvailPageFile", C.c_size_t),
                        ("dwTotalVirtual", C.c_size_t),
                        ("dwAvailVirtual", C.c_size_t),
                        ("_pad", C.c_uint64)]

        if os.name != "nt":
            return None
        ms = MEMORYSTATUSEx()
        ms.dwLength = C.sizeof(MEMORYSTATUSEx)
        if not C.windll.kernel32.GlobalMemoryStatusEx(C.byref(ms)):
            return None
        used = int(ms.dwTotalPhys) - int(ms.dwAvailPhys)
        if used < 0:
            return None
        return round(used / (1024 * 1024), 1)
    except Exception:
        return None


def llama_idle(cfg, state, agent_window_s: int | None = None) -> bool:
    """True when the llama model is NOT meaningfully active (idle or absent).

    "Active" = CPU fraction at/above stall_llama_cpu_frac (10% default), OR a
    llama-server slot is currently processing, OR tokens are currently being
    decoded or prompt-processed (output tps or ingest tps > 0). The slots and
    token-speed checks cover the heavy-GPU-offload case where the CPU looks
    idle while text is still being generated.

    The GPU gate (stall_gate_gpu) is a FALLBACK ONLY: gpu_engine_max() is
    machine-wide (all processes — this WDDM box exposes no per-process GPU
    counter), so desktop/compositor/other-app noise sits right at the 10%
    threshold and false-trips it for minutes at a time (observed 2026-09-22:
    blocked every poll 17:42-18:01 while llama was verifiably idle). When the
    direct signals (slots/tokens) are enabled they fully cover generation, so
    the GPU reading is consulted only when BOTH are disabled (server builds
    that expose neither)."""
    frac = llama_cpu_frac(cfg, state, agent_window_s)
    thresh = float(cfg.get("stall_llama_cpu_frac", 0.10))
    if frac >= thresh:
        return False
    # CPU looks idle, but under heavy GPU offload the CPU can be quiet while
    # text is still being generated. Ask llama-server directly.
    slots_on = bool(cfg.get("stall_gate_slots", True))
    if slots_on and llama_slots_processing(cfg):
        return False
    # Token speeds: a generation that is mid-decode moves decoded_tokens even
    # when  is_processing is not reported (varies by server version). Both
    # output AND ingest speeds must be zero for the model to count as idle.
    tokens_on = bool(cfg.get("stall_gate_tokens", True))
    if tokens_on:
        out_tps, ingest_tps = llama_token_speeds(cfg)
        if out_tps > 0.0 or ingest_tps > 0.0:
            return False
    # Machine-wide GPU reading — consulted ONLY when the direct signals above
    # are unavailable (see docstring). Never as an independent veto.
    if cfg.get("stall_gate_gpu", True) and not (slots_on or tokens_on):
        gpu = llama_gpu_util(cfg, state)
        if gpu and float(gpu.get("max", 0.0)) >= \
                float(cfg.get("stall_llama_gpu_util", 10.0)):
            return False
    return True


def llama_idle_sustained(cfg, state) -> bool:
    """llama_idle confirmed by a second sample stall_idle_confirm_gap_s later.

    The local llama-server is SHARED with the coordinator's own opencode
    instance (this session and small_model chatter — compaction, titles — all
    hit the same server/slots as the worker), so a single instantaneous busy
    sample is usually a short blip of OUR OWN harness, not the worker
    generating. The injection gates therefore require the server to be busy on
    BOTH samples: a real worker turn (minutes long) survives both, a
    compaction/title blip (seconds) usually does not. gap <= 0 = old single
    sample behavior."""
    if llama_idle(cfg, state):
        return True
    gap = int(cfg.get("stall_idle_confirm_gap_s", 30))
    if gap <= 0:
        return False
    time.sleep(min(gap, 55))
    return llama_idle(cfg, state)


def build_continue_cmd(cli, attach, session_id, project_dir, message, auto=True,
                       creds=None):
    # Both paths pass --auto unless auto=False: "auto-approve permissions that
    # are not explicitly denied", so Hawk-driven continues never block on an
    # access prompt. auto=False (an answer/critical injection) omits it.
    if attach:
        # Attach to the Desktop's own sidecar so the turn is generated there and
        # the GUI streams it live (requirement: user sees worker progress).
        cmd = [cli, "run", "--attach", attach, "--session", session_id]
    else:
        cmd = [cli, "run", "--session", session_id]
    if auto:
        cmd.append("--auto")
    if attach and creds:
        # The Desktop sidecar enforces HTTP Basic auth with per-launch creds
        # (desktop_sidecar()); without them the attach dies with 401 and the
        # injected message never lands. The CLI defaults the username to
        # "opencode", but pass both explicitly for robustness.
        cmd += ["--username", creds.get("username") or "opencode",
                "--password", creds.get("password") or ""]
    cmd += ["--dir", project_dir, message]
    return cmd


def send_continue(cfg, session_id, project_dir, message, auto=True) -> bool:
    cli = find_opencode()
    if not cli:
        return False
    sidecar = None
    if cfg.get("continue_via_attach", True):
        # desktop_sidecar() resolves the listener AND its per-launch Basic auth
        # creds in one call (find_desktop_server() alone would attach without
        # creds and be 401-rejected - see build_continue_cmd).
        sidecar = desktop_sidecar()
    attach = sidecar[0] if sidecar else None
    creds = sidecar[1] if sidecar else None
    cmd = build_continue_cmd(cli, attach, session_id, project_dir, message,
                             auto=auto, creds=creds)
    log("injecting continue via %s" %
        ("attach %s" % attach if attach else "standalone server (desktop sidecar not found)"))
    log("cmd: %s" % " ".join(cmd[:6]))
    try:
        flags = creation_flags()
        # Fire-and-forget: the CLI = passive client in attach mode; the Desktop
        # sidecar owns the turn. We never block on the full turn (local models
        # can take many minutes); the next polls see the session busy.
        subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            creationflags=flags,
        )
        return True
    except Exception as e:
        log("continue injection error: %s" % e)
        return False


def _session_project_dir(session_id: str) -> str:
    """Resolve the active leaf session's project directory for continue
    injection (mirrors the lookup poll_multisession performs)."""
    try:
        con = connect_db(db_path())
        try:
            base = session_row(con, session_id)
            if base is None:
                return ""
            leaf = active_leaf(con, base)
            return (leaf.get("directory") or "").strip()
        finally:
            con.close()
    except Exception:
        return ""


def _reply_to_session(cfg, session_id: str, choice: str) -> bool:
    """Deliver a mobile (ntfy) reply back into the session as a continue.
    The raw text is injected 1:1 without wrapping."""
    try:
        project_dir = _session_project_dir(session_id) or cfg.get("project_dir") or ""
        if not project_dir:
            log("notify: cannot reply to %s: no project dir" % session_id)
            return False
        ok = send_continue(cfg, session_id, project_dir, choice, auto=False)
        log("notify: ntfy reply %r -> %s %s"
            % (choice[:60], session_id, "OK" if ok else "FAILED"))
        return ok
    except Exception as e:
        log("notify: reply-to-session failed: %s" % e)
        return False


# ── Escalation ───────────────────────────────────────────────────────────────
def write_escalation(session, project_dir, reason, rule_hits, analysis, cfg) -> Path:
    esc_dir = home_dir() / "escalations"
    esc_dir.mkdir(parents=True, exist_ok=True)
    path = esc_dir / ("escalation_%s_%s.md" % (utcnow().strftime("%Y%m%d_%H%M%S"),
                                                session["id"][-6:]))
    lines = []
    lines.append("# Coordinator escalation\n")
    lines.append("- Time: %s" % utcnow().isoformat())
    lines.append("- Session: %s (%s)" % (session["id"], session.get("title", "")))
    lines.append("- Reason: %s\n" % reason)
    lines.append("## Rules triggered")
    for r in rule_hits:
        lines.append("- %s" % r)
    lines.append("\n## Last assistant message")
    lines.append("```\n%s\n```" % (analysis["last_assistant"][-2000:] or "(none)"))
    lines.append("\n## Recent tool output tail")
    lines.append("```\n%s\n```" % (analysis["tool_blob"][-2000:] or "(none)"))
    lines.append("\n## What to do")
    lines.append("- Read the transcript; if this was a genuine milestone completion, "
                 "reply `continue` manually, or add the missing evidence (commit / STOP: VERIFIED).")
    lines.append("- If a decision is required, make it, then let the worker continue.")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return path


def _gen_opencode_id(prefix: str) -> str:
    return prefix + uuid.uuid4().hex


def _model_field(session) -> dict:
    """Reshape the session.model column ({id,providerID,variant}) into the
    {providerID, modelID} shape opencode records on a message's model field."""
    raw = session.get("model")
    try:
        m = json.loads(raw) if isinstance(raw, str) else (raw or {})
        if not isinstance(m, dict):
            m = {}
    except Exception:
        m = {}
    return {"providerID": m.get("providerID", "llama.cpp"),
            "modelID": m.get("id") or m.get("modelID", "")}


def build_escalation_note(session, reason, rule_hits, esc_path) -> str:
    stamp = datetime.now().astimezone().isoformat(timespec="minutes")
    hits = ", ".join(rule_hits) if rule_hits else "(none)"
    return (
        "[hawk escalation] %s — the local milestone coordinator stopped auto-"
        "continuing this session.\nReason: %s\nRules: %s\nFull report: %s\n"
        "If this is a genuine milestone completion, reply `continue`; "
        "otherwise add the missing evidence (commit / STOP: VERIFIED) or make "
        "the required decision."
        % (stamp, reason, hits, esc_path)
    )


def write_plan_done(session, project_dir, reason, rule_hits, analysis, cfg) -> Path:
    """Positive counterpart of write_escalation(): the build plan finished. No
    'add the missing evidence / reply continue to resume' wording — the worker
    is done, so we tell the human and stop auto-continuing."""
    done_dir = home_dir() / "escalations"
    done_dir.mkdir(parents=True, exist_ok=True)
    path = done_dir / ("done_%s_%s.md" % (utcnow().strftime("%Y%m%d_%H%M%S"),
                                         session["id"][-6:]))
    lines = []
    lines.append("# Build plan complete\n")
    lines.append("- Time: %s" % utcnow().isoformat())
    lines.append("- Session: %s (%s)" % (session["id"], session.get("title", "")))
    lines.append("- Reason: %s\n" % reason)
    lines.append("## Rules triggered")
    for r in rule_hits:
        lines.append("- %s" % r)
    lines.append("\n## Last assistant message")
    lines.append("```\n%s\n```" % (analysis["last_assistant"][-2000:] or "(none)"))
    lines.append("\n## What to do")
    lines.append("- The build plan appears complete; the coordinator will NOT send "
                 "further auto-continues for this session.")
    lines.append("- Start a new task when ready, or reply if something is unfinished.")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return path


def build_plan_done_note(session, reason, rule_hits, done_path) -> str:
    stamp = datetime.now().astimezone().isoformat(timespec="minutes")
    hits = ", ".join(rule_hits) if rule_hits else "(none)"
    return (
        "[hawk] Build plan complete — the local milestone coordinator stopped "
        "auto-continuing this session.\nStopped at: %s\nReason: %s\nRules: %s\n"
        "Report: %s\n"
        "No further auto-continues will be sent. Start a new task when ready."
        % (stamp, reason, hits, done_path)
    )


def append_session_note(db: Path, session_id: str, text: str, model: dict) -> bool:
    """Insert a synthetic user message + text part into the opencode DB so the
    escalation is visible in the session transcript (the same shape opencode
    uses for its own injected prompts). Returns False and never raises on
    failure — an unrecorded escalation is not fatal."""
    now = int(time.time() * 1000)
    mid = _gen_opencode_id("msg_")
    pid = _gen_opencode_id("prt_")
    msg_data = {"role": "user", "time": {"created": now}, "agent": "build", "model": model}
    part_data = {"type": "text", "synthetic": True, "text": text,
                 "time": {"start": now, "end": now}}
    try:
        con = sqlite3.connect(str(db), timeout=5)
        try:
            con.execute("PRAGMA busy_timeout=4000")
            con.execute(
                "INSERT INTO message (id, session_id, time_created, time_updated, data) "
                "VALUES (?,?,?,?,?)",
                (mid, session_id, now, now, json.dumps(msg_data)))
            con.execute(
                "INSERT INTO part (id, message_id, session_id, time_created, "
                "time_updated, data) VALUES (?,?,?,?,?,?)",
                (pid, mid, session_id, now, now, json.dumps(part_data)))
            con.commit()
        finally:
            con.close()
        return True
    except sqlite3.Error as e:
        log("warn: could not record escalation in session DB: %s" % e)
        return False


# ── Self-check rule engine ───────────────────────────────────────────────────
def pending_question_block(parts) -> bool:
    """True when the session's current turn is blocked awaiting an answer to a
    `question`-tool request (data.tool == "question" with state.status still
    "running"). The self-check continue is useless here: the worker cannot
    process an injected prompt until the question is answered, and re-injecting
    it on every re_stall cycle just stacks dead user messages into the context.
    The Desktop question sweep (desktop_question_sweep) handles the unblock."""
    if not parts:
        return False
    for p in parts:
        d = p.get("data") or {}
        if d.get("type") == "tool" and d.get("tool") == "question":
            return (d.get("state") or {}).get("status") == "running"
    return False


def evaluate(cfg, session, parts, messages, state):
    """Return (action, reason, rule_hits).

    The coordinator never judges the worker's output.  It injects one
    self-check prompt when a turn has stopped (a completed message, or an
    unfinished turn silent past stalled_turn_minutes) and has not already
    been contacted within re_stall_minutes.  The worker answers either by
    continuing to work, or with STOP: DONE.

    STOP: DONE needs a double-tap: the FIRST occurrence is met with a short
    "are you sure?" confirmation prompt (done_confirm_message - not the
    self-check continue text, and only once per DONE).  Only a SECOND STOP: DONE
    on a *new* message registers the session as finished (done action).  Any
    other reply clears the pending confirmation and the normal loop resumes, so
    an LLM that stops too eagerly is nudged back to work instead of closing the
    plan on a single token."""
    now_ms = int(time.time() * 1000)
    last_mid = messages[-1]["id"] if messages else None
    last_t = last_part_time(parts)
    recent_activity = bool(last_t) and (now_ms - last_t) < cfg["idle_minutes"] * 60_000
    stall_s = int(cfg.get("stalled_turn_minutes", 15)) * 60_000
    stale_wedge = bool(
        busy_from_messages(messages)  # an unfinished turn
        and parts
        and (now_ms - last_t) > stall_s)

    # A pending question-tool request blocks the turn: the worker cannot act
    # on an injected continue until a selection arrives, so send nothing (the
    # Desktop question sweep answers it instead). Must be logged/suppressed
    # even when the wedge has expired and llama is idle.
    if pending_question_block(parts):
        return "nothing", "session busy (pending question selection)", []

    # A completed turn at rest past idle may still need a re-stall
    # follow-up (a session that keeps "stopping" gets contacted, but never
    # hammered - the cadence below bounds re-injection). Let it through to
    # that logic; anything else already evaluated on this message stands
    # down.
    re_stall_candidate = (parts and not busy_from_messages(messages)
                          and not recent_activity)
    if last_mid and state.get("last_evaluated_mid") == last_mid \
            and not stale_wedge and not re_stall_candidate:
        return "nothing", "already evaluated", []

    if recent_activity:
        return "nothing", "session busy (recent activity)", []

    a = analyze(parts, messages)

    # ── Post-done guard: suppress re-confirmation ────────────────────────────
    # After a DONE + append_session_note, the synthetic note creates a new
    # message → grew=True → evaluate re-runs → sees old STOP: DONE in last
    # assistant text → would re-set pending_done_mid and inject confirm again.
    # Guard: if done_mid is recorded and the STOP: DONE marker comes from that
    # same message, return nothing.  If the worker sent new work (plan_done
    # False on a newer message), clear done_mid — session reactivated.
    done_mid = state.get("done_mid") or ""
    if done_mid:
        cur_done_mid = (a.get("plan_done_mid") or last_mid or "") if a["plan_done"] else ""
        if a["plan_done"] and cur_done_mid == done_mid:
            return "nothing", "build already marked complete", []
        # Worker sent a new message (not the done mid) — resume normal flow
        if last_mid and last_mid != done_mid:
            state.pop("done_mid", None)
        # Fall through (old done_mid cleared or same done_mid but no plan_done)

    # ── STOP: DONE double-tap ──────────────────────────────────────────────
    # First STOP: DONE -> ask "are you sure?" (once, without continue text).
    # The confirmation is accepted ONLY when a new STOP: DONE appears on a
    # different message id (plan_done_mid != pending_done_mid). Anything else
    # clears the pending confirmation and the normal self-check loop resumes.
    pending_done = state.get("pending_done_mid") or ""
    if a["plan_done"]:
        done_mid = a.get("plan_done_mid") or last_mid or ""
        if pending_done and pending_done != done_mid:
            # Second, distinct STOP: DONE -> worker genuinely finished.
            state.pop("pending_done_mid", None)
            return "done", ("worker confirmed build plan complete (%s)" % a["plan_done_marker"]), [a["plan_done_marker"]]
        if not pending_done:
            # First STOP: DONE -> one-time confirmation, no continue text.
            state["pending_done_mid"] = done_mid
            return "confirm_done", ("worker says build plan complete (%s); asking once to confirm" % a["plan_done_marker"]), [a["plan_done_marker"]]
        # The same STOP: DONE message is already pending and was already asked
        # about. Fall through to the cadence logic below - do not re-ask on the
        # same message.
    elif pending_done:
        # Worker sent something new that is NOT STOP: DONE -> back to work.
        state.pop("pending_done_mid", None)

    # An unfinished turn that is still generating: leave it alone. A long single
    # generation may not write new parts for a while; only treat it as stopped
    # once it is both silent past the wedge threshold AND the model is not
    # actively decoding.
    if busy_from_messages(messages):
        if parts and (now_ms - last_t) > stall_s:
            if not llama_idle_sustained(cfg, state):
                return "nothing", "session busy (llama active, no new parts yet)", []
            # Llama idle, turn stalled long enough: fall through to self-check.
        else:
            return "nothing", "session busy (unfinished turn)", []

    # Liveness watchdog: if llama-server is dead, trigger a restart IMMEDIATELY,
    # bypassing all cooldown/cadence logic. This was the gap that caused the
    # 2026-09-22 22:02 silent-downtime incident (server died during cooldown).
    if not llama_server_alive(cfg):
        return "llama_down", "llama-server process not found; triggering respawn", []

    # Stopped turn (completed, at rest long enough to clear "busy"). One
    # injection per re_stall_minutes so a session that keeps "stopping"
    # gets contacted, but never hammered.
    re_stall_ms = int(cfg.get("re_stall_minutes", 45)) * 60_000
    last_inj = int(state.get("last_injected_at") or 0)
    if last_inj and (now_ms - last_inj) < re_stall_ms:
        return "nothing", ("already asked at %s; next follow-up in %d min"
                           % (time.strftime("%H:%M:%S", time.localtime(last_inj / 1000.0)),
                              int((re_stall_ms - (now_ms - last_inj)) / 60_000))), []

    # Same stand-down as the unfinished-turn path: even a COMPLETED turn must
    # not be nudged while llama is mid-generation (another session's turn, or
    # this session's turn still finishing on the server). Only inject when the
    # model is truly idle — and busy must be confirmed by a second sample
    # (llama_idle_sustained) so a short blip from the coordinator's own
    # opencode harness (compaction/titles on the shared server) does not
    # block the injection indefinitely.
    if not llama_idle_sustained(cfg, state):
        return "nothing", "worker stopped but llama active; waiting for idle", []

    if pending_done:
        # A confirmation of STOP: DONE is pending but the worker never answered
        # it. After the cadence, re-ask once - never with the self-check
        # continue text, and never on a fresh turn (that would defeat the
        # double-tap). Keeps the confirm-then-die loop alive without hammering.
        return "confirm_done", "confirmation of STOP: DONE still unanswered; asking once more", []

    return "continue", "worker stopped; sending self-check prompt", []


# ── Poll pass ────────────────────────────────────────────────────────────────
def self_test():
    """Hermetic wrapper: scenarios must not depend on whether a real
    llama-server happens to be running on this machine. The liveness probe
    (tasklist) is stubbed to "alive"; the llama_down scenarios override it
    explicitly. The real probe is restored afterwards."""
    g = globals()
    real_alive = g["llama_server_alive"]
    g["llama_server_alive"] = lambda cfg: True
    g["_REAL_LLAMA_SERVER_ALIVE"] = real_alive   # for the LW1 unit check
    try:
        return _self_test_impl()
    finally:
        g["llama_server_alive"] = real_alive


def _self_test_impl():
    """Exercise the self-check rule engine on synthetic scenarios; no DB, no network."""
    import tempfile

    def mk_repo():
        d = tempfile.mkdtemp()
        subprocess.run(["git", "-C", d, "init", "-q"], check=True)
        subprocess.run(["git", "-C", d, "config", "user.email", "t@t"], check=True)
        subprocess.run(["git", "-C", d, "config", "user.name", "t"], check=True)
        (Path(d) / "src").mkdir()
        (Path(d) / "src" / "lib.rs").write_text("pub fn x() {}\n", encoding="utf-8")
        subprocess.run(["git", "-C", d, "add", "-A"], check=True)
        subprocess.run(["git", "-C", d, "commit", "-qm", "init"], check=True)
        return d

    def mkcfg(d):
        c = json.loads(json.dumps(CONFIG_DEFAULTS))
        c.update(project_dir=d, idle_minutes=8)
        return c

    def mkparts(final_text, tool_outputs=(), last_age_s=600):
        now = int(time.time() * 1000)
        parts = []
        msgs = []
        for i, out in enumerate(tool_outputs):
            parts.append({"time": now - last_age_s * 1000 + i,
                          "type": "tool", "text": "",
                          "data": {"type": "tool", "output": out}})
        parts.append({"time": now - last_age_s * 1000 + len(tool_outputs) + 1,
                      "type": "text", "text": final_text, "data": {"type": "text"}})
        msgs.append({"id": "msg_test_final", "time": now,
                     "time_data": {"created": now, "completed": now},
                     "role": "assistant"})
        return parts, msgs

    results = []
    def run(name, expect, parts, msgs, state):
        d = mk_repo()
        state = dict(state)
        orig_idle = evaluate.__globals__["llama_idle"]
        try:
            evaluate.__globals__["llama_idle"] = (lambda c, s, w=None: True)
            action, reason, hits = evaluate(mkcfg(d), {"id": "ses_test", "title": "t"},
                                            parts, msgs, state)
        finally:
            evaluate.__globals__["llama_idle"] = orig_idle
            shutil.rmtree(d, ignore_errors=True)
        ok = action == expect
        results.append((name, expect, action, ok, reason, hits))
        print("%-22s expect=%-9s got=%-9s %s  %s" % (
            name, expect, action, "OK" if ok else "MISMATCH", reason))

    # G: llama_idle gate composition (sub-signals mocked; no network).
    #    Regression: the machine-wide GPU reading must NOT veto idleness when
    #    the direct slots/tokens signals are enabled (2026-09-22 false-busy:
    #    desktop noise at ~11% GPU blocked every poll for ~20 min while llama
    #    was verifiably idle). GPU is a fallback for servers that expose
    #    neither slots nor token counters.
    def run_idle(name, expect, cfg_over, cpu, slots, toks, gpu):
        c = json.loads(json.dumps(CONFIG_DEFAULTS))
        c.update(cfg_over)
        g = globals()
        orig = {k: g[k] for k in ("llama_cpu_frac", "llama_slots_processing",
                                  "llama_token_speeds", "llama_gpu_util")}
        try:
            g["llama_cpu_frac"] = lambda c2, s, w=None: cpu
            g["llama_slots_processing"] = lambda c2: slots
            g["llama_token_speeds"] = lambda c2: toks
            g["llama_gpu_util"] = lambda c2, s=None: {"sum": gpu, "max": gpu}
            got = llama_idle(c, {})
        finally:
            g.update(orig)
        ok = got == expect
        results.append((name, expect, got, ok, "gate composition", []))
        print("%-22s expect=%-5s got=%-5s %s" % (
            name, expect, got, "OK" if ok else "MISMATCH"))

    run_idle("g_idle_all", True, {}, 0.0, False, (0.0, 0.0), 0.0)
    run_idle("g_cpu_busy", False, {}, 0.5, False, (0.0, 0.0), 0.0)
    run_idle("g_slots_busy", False, {}, 0.0, True, (0.0, 0.0), 0.0)
    run_idle("g_tokens_busy", False, {}, 0.0, False, (5.0, 0.0), 0.0)
    run_idle("g_gpu_noise_ignored", True, {}, 0.0, False, (0.0, 0.0), 42.0)
    run_idle("g_gpu_fallback_busy", False,
             {"stall_gate_slots": False, "stall_gate_tokens": False},
             0.0, False, (0.0, 0.0), 42.0)
    run_idle("g_gpu_fallback_idle", True,
             {"stall_gate_slots": False, "stall_gate_tokens": False},
             0.0, False, (0.0, 0.0), 2.0)

    # G2: llama_idle_sustained (2-sample confirmation against own-harness blips
    #     on the shared local server; regression 2026-09-22: compaction/title
    #     small-model blips kept single-sample polls "llama active" forever).
    def run_sustained(name, expect, slot_seq, gap=1, want_calls=None):
        c = json.loads(json.dumps(CONFIG_DEFAULTS))
        c["stall_idle_confirm_gap_s"] = gap
        g = globals()
        orig = {k: g[k] for k in ("llama_cpu_frac", "llama_slots_processing",
                                  "llama_token_speeds", "llama_gpu_util")}
        calls = [0]
        try:
            g["llama_cpu_frac"] = lambda c2, s, w=None: 0.0
            g["llama_slots_processing"] = lambda c2: (
                calls.__setitem__(0, calls[0] + 1),
                slot_seq[min(calls[0] - 1, len(slot_seq) - 1)])[1]
            g["llama_token_speeds"] = lambda c2: (0.0, 0.0)
            g["llama_gpu_util"] = lambda c2, s=None: {"sum": 0.0, "max": 0.0}
            got = llama_idle_sustained(c, {})
        finally:
            g.update(orig)
        ok = got == expect and (want_calls is None or calls[0] == want_calls)
        results.append((name, expect, got, ok, "sustained idle", []))
        print("%-22s expect=%-5s got=%-5s calls=%d %s" % (
            name, expect, got, calls[0], "OK" if ok else "MISMATCH"))

    run_sustained("s_idle_first", True, [False, True], want_calls=1)
    run_sustained("s_busy_then_idle", True, [True, False])
    run_sustained("s_busy_both", False, [True, True])
    run_sustained("s_gap_zero_single", False, [True], gap=0, want_calls=1)

    now = int(time.time() * 1000)

    # A: busy (recent part) -> nothing
    parts, msgs = mkparts("Almost there...", last_age_s=1)
    run("busy", "nothing", parts, msgs, {})

    # A2: a pending question-tool selection blocks the turn -> NOTHING, never a
    #     continue (the Desktop question sweep answers it instead)
    now = int(time.time() * 1000)
    parts_q = [{"time": now - 30 * 60 * 1000, "message_id": "msg_q", "role": "assistant",
                "type": "tool", "text": "",
                "data": {"type": "tool", "tool": "question",
                         "state": {"status": "running"}}}]
    msgs_q = [{"id": "msg_q", "time": now - 30 * 60 * 1000, "role": "assistant",
               "time_data": {"created": now - 30 * 60 * 1000}}]  # unfinished turn
    run("question_blocks", "nothing", parts_q, msgs_q, {})

    # A3: once the question is answered (status completed), continue resumes
    parts_c = [{"time": now - 30 * 60 * 1000, "message_id": "msg_q2", "role": "assistant",
                "type": "tool", "text": "",
                "data": {"type": "tool", "tool": "question",
                         "state": {"status": "completed"}}},
               {"time": now - 20 * 60 * 1000, "message_id": "msg_done", "role": "assistant",
                "type": "text", "text": "Answered; no further questions.",
                "data": {"type": "text"}}]
    msgs_c = [{"id": "msg_q2", "time": now - 30 * 60 * 1000, "role": "assistant",
               "time_data": {"created": now - 30 * 60 * 1000, "completed": now - 30 * 60 * 1000}},
              {"id": "msg_done", "time": now - 20 * 60 * 1000, "role": "assistant",
               "time_data": {"created": now - 20 * 60 * 1000, "completed": now - 20 * 60 * 1000}}]
    run("question_answered", "continue", parts_c, msgs_c,
        {"last_injected_at": now - 60 * 60 * 1000})

    # B: completed turn, stopped normally -> self-check (no gate; the worker
    #    decides when to say DONE)
    parts, msgs = mkparts("Done with C7.1.")
    run("stopped_continue", "continue", parts, msgs, {})

    # C: STOP: VERIFIED is not STOP: DONE -> continue (worker chose to keep going)
    parts, msgs = mkparts("Milestone complete. STOP: VERIFIED")
    run("verified_continue", "continue", parts, msgs, {})

    # D: dirty tree does not block (no gate; only STOP: DONE matters)
    d = mk_repo()
    (Path(d) / "src" / "lib.rs").write_text("pub fn x() {}\npub fn y() {}\n", encoding="utf-8")
    parts, msgs = mkparts("Milestone complete. STOP: VERIFIED")
    ok_f = False
    orig_idle_f = evaluate.__globals__["llama_idle"]
    try:
        evaluate.__globals__["llama_idle"] = (lambda c, s, w=None: True)
        action, reason, hits = evaluate(mkcfg(d), {"id": "s", "title": "t"}, parts, msgs, {})
        ok_f = action == "continue"
    finally:
        evaluate.__globals__["llama_idle"] = orig_idle_f
        shutil.rmtree(d, ignore_errors=True)
    results.append(("dirty_tree_ignored", "continue", action, ok_f, reason, hits))
    print("%-22s expect=%-9s got=%-9s %s  %s" % (
        "dirty_tree_ignored", "continue", action, "OK" if ok_f else "MISMATCH", reason))

    # E: first STOP: DONE -> ask once to confirm (double-tap), not done yet
    parts, msgs = mkparts("All tasks in the plan are complete. STOP: DONE")
    run("done_first_tap", "confirm_done", parts, msgs, {})
    # E2: the same DONE message re-evaluated must not re-ask or finish
    parts, msgs = mkparts("All tasks in the plan are complete. STOP: DONE")
    run("done_same_msg", "nothing", parts, msgs,
        {"pending_done_mid": "msg_test_final",
         "last_injected_at": int(time.time() * 1000) - 10 * 60 * 1000})
    # E3: a SECOND, distinct STOP: DONE message -> confirmed, plan done
    now = int(time.time() * 1000)
    parts2 = [{"time": now - 2400000, "message_id": "msg_done1", "role": "assistant",
               "type": "text", "text": "Plan complete. STOP: DONE",
               "data": {"type": "text"}},
              {"time": now - 1800000, "message_id": "msg_confirm_ask", "role": "user",
               "type": "text", "text": "[coordinator] Noted. If every sub-task...",
               "data": {"type": "text"}},
              {"time": now - 1200000, "message_id": "msg_done2", "role": "assistant",
               "type": "text", "text": "Confirmed. STOP: DONE",
               "data": {"type": "text"}}]
    msgs2 = [{"id": "msg_done1", "time": now - 2400000, "role": "assistant",
              "time_data": {"created": now - 2400000, "completed": now - 2400000}},
             {"id": "msg_confirm_ask", "time": now - 1800000, "role": "user",
              "time_data": {"created": now - 1800000}},
             {"id": "msg_done2", "time": now - 1200000, "role": "assistant",
              "time_data": {"created": now - 1200000, "completed": now - 1200000}}]
    ok_e3 = False
    reason_e3, hits_e3 = "", []
    d_e3 = mk_repo()
    orig_idle_e3 = evaluate.__globals__["llama_idle"]
    try:
        evaluate.__globals__["llama_idle"] = (lambda c, s, w=None: True)
        action_e3, reason_e3, hits_e3 = evaluate(
            mkcfg(d_e3), {"id": "s", "title": "t"}, parts2, msgs2,
            {"pending_done_mid": "msg_done1",
             "last_injected_at": int(time.time() * 1000) - 60 * 60 * 1000})
        ok_e3 = action_e3 == "done"
    finally:
        evaluate.__globals__["llama_idle"] = orig_idle_e3
        shutil.rmtree(d_e3, ignore_errors=True)
    results.append(("done_second_tap", "done", action_e3, ok_e3, reason_e3, hits_e3))
    print("%-22s expect=%-9s got=%-9s %s  %s" % (
        "done_second_tap", "done", action_e3, "OK" if ok_e3 else "MISMATCH", reason_e3))

    # E4: a pending confirmation that goes unanswered is re-asked after cadence
    #     (never finished, never re-sent with the continue text)
    parts, msgs = mkparts("All tasks in the plan are complete. STOP: DONE")
    run("done_pending_requeue", "confirm_done", parts, msgs,
        {"pending_done_mid": "msg_test_final",
         "last_injected_at": int(time.time() * 1000) - 60 * 60 * 1000})

    # E5: post-done guard — the DONE note creates a new message, so a re-eval
    #     would re-confirm; done_mid suppresses it (no re-injection)
    parts, msgs = mkparts("All tasks in the plan are complete. STOP: DONE")
    run("done_stays_done", "nothing", parts, msgs,
        {"done_mid": "msg_test_final"})
    # E6: worker resumed work after done -> done_mid cleared, normal loop
    parts, msgs = mkparts("Found more work to do.")
    run("done_resumed", "continue", parts, msgs,
        {"done_mid": "msg_old_done",
         "last_injected_at": int(time.time() * 1000) - 60 * 60 * 1000})

    # F: self-check throttle: injected recently -> nothing (no spam)
    parts, msgs = mkparts("Done with milestone.")
    run("throttle_active", "nothing", parts, msgs,
        {"last_injected_at": int(time.time() * 1000) - 10 * 60 * 1000})

    # G: throttle elapsed -> continue
    parts, msgs = mkparts("Done with milestone.")
    run("throttle_elapsed", "continue", parts, msgs,
        {"last_injected_at": int(time.time() * 1000) - 60 * 60 * 1000})

    # H: control channel (pause/resume/poll_now, seq-guarded via COORD_HOME)
    d = tempfile.mkdtemp()
    try:
        old_home = os.environ.get("COORD_HOME")
        os.environ["COORD_HOME"] = d
        try:
            st = {"control_seq": 0}
            (Path(d) / "control.json").write_text(
                json.dumps({"intent": "poll_now", "seq": 1, "at": 0}), encoding="utf-8")
            got = apply_control(None, st)
            ok_h1 = got is True and st["control_seq"] == 1 and "last_poll_now" in st
            got = apply_control(None, st)  # duplicate seq is ignored
            ok_h2 = got is False and st["control_seq"] == 1
            (Path(d) / "control.json").write_text(
                json.dumps({"intent": "resume", "seq": 2, "at": 0}), encoding="utf-8")
            st["paused"] = True
            apply_control(None, st)
            ok_h3 = st["control_seq"] == 2 and st.get("paused") is False
            (Path(d) / "control.json").write_text(
                json.dumps({"intent": "pause", "seq": 3, "at": 0}), encoding="utf-8")
            apply_control(None, st)
            ok_h4 = st["control_seq"] == 3 and st.get("paused") is True
        finally:
            if old_home is None:
                os.environ.pop("COORD_HOME", None)
            else:
                os.environ["COORD_HOME"] = old_home
    finally:
        shutil.rmtree(d, ignore_errors=True)
    results.append(("control_channel", "pass", "pass", all([ok_h1, ok_h2, ok_h3, ok_h4]),
                    "control seq/pause/resume/poll_now", []))
    print("%-22s expect=%-9s got=%-9s %s  %s" % (
        "control_channel", "pass", "pass", "OK" if all([ok_h1, ok_h2, ok_h3, ok_h4])
        else "MISMATCH", "control seq/pause/resume/poll_now"))

    # I: stalled turn: only action is llama stand-down (idle -> continue; busy -> nothing)
    def mkstall(llama_pid_val=None, llama_active=False):
        d = mk_repo()
        cfg = mkcfg(d)
        cfg["llama_pid"] = llama_pid_val if llama_pid_val is not None else 99999999
        old = time.time() * 1000 - 30 * 60 * 1000
        parts = [{"time": int(old), "type": "text", "text": "working...",
                  "data": {"type": "text"}}]
        msgs = [{"id": "msg_unfinished", "time": int(old),
                 "time_data": {"created": int(old)},
                 "role": "assistant"}]
        orig = evaluate.__globals__["llama_idle"]
        try:
            evaluate.__globals__["llama_idle"] = (
                (lambda c, s, w=None: False) if llama_active
                else (lambda c, s, w=None: True))
            action, reason, hits = evaluate(cfg, {"id": "s", "title": "t"},
                                            parts, msgs, {})
        finally:
            evaluate.__globals__["llama_idle"] = orig
            shutil.rmtree(d, ignore_errors=True)
        return action
    ok_i1 = mkstall(llama_active=False) == "continue"   # idle -> send self-check
    ok_i2 = mkstall(llama_active=True) == "nothing"     # busy -> stand down
    results.append(("llama_stall_gate", "pass", "pass", ok_i1 and ok_i2,
                    "stall: continue only when llama idle", []))
    print("%-22s expect=%-9s got=%-9s %s  %s" % (
        "llama_stall_gate", "pass", "pass", "OK" if (ok_i1 and ok_i2) else "MISMATCH",
        "stall: continue only when llama idle"))

    # M: permanently-wedged turn must never go silent. "already evaluated" must
    #    not swallow a still-stale turn, and re_stall cadence re-fires.
    ok_m = True
    d_m = mk_repo()
    cfg_m = mkcfg(d_m)
    cfg_m["llama_pid"] = 99999999
    old_m = int(time.time()) * 1000 - 20 * 60 * 1000
    parts_m = [{"time": old_m, "type": "text", "text": "working...", "data": {"type": "text"}}]
    msgs_m = [{"id": "msg_wedged", "time": old_m,
               "time_data": {"created": old_m}, "role": "assistant"}]
    orig_ml = evaluate.__globals__["llama_idle"]
    try:
        evaluate.__globals__["llama_idle"] = (lambda c, s, w=None: True)
        # (1) already-evaluated but STILL wedged -> must not swallow; continues
        st_m = {"last_evaluated_mid": "msg_wedged", "last_injected_at": 0}
        action_m1, reason_m1, _h = evaluate(cfg_m, {"id": "s", "title": "t"},
                                            parts_m, msgs_m, st_m)
        ok_m = ok_m and action_m1 == "continue" and "already evaluated" not in reason_m1
        # (2) throttle active -> nothing with follow-up cadence
        st_m["last_injected_at"] = int(time.time() * 1000) - 5 * 60 * 1000
        action_m2, reason_m2, _h = evaluate(cfg_m, {"id": "s", "title": "t"},
                                            parts_m, msgs_m, st_m)
        ok_m = ok_m and action_m2 == "nothing" and "next follow-up" in reason_m2
        # (3) re_stall_minutes elapsed -> continue again
        st_m["last_injected_at"] = int(time.time() * 1000) - 60 * 60 * 1000
        action_m3, reason_m3, _h = evaluate(cfg_m, {"id": "s", "title": "t"},
                                            parts_m, msgs_m, st_m)
        ok_m = ok_m and action_m3 == "continue"
        # (4) hard stop past 2x stall: still continue (throttle governs, not
        #     a hard budget); the stand-down path no longer escalates.
        cfg_m["stalled_turn_minutes"] = 10
        very_old_m = int(time.time()) * 1000 - 30 * 60 * 1000
        parts_m2 = [{"time": very_old_m, "type": "text", "text": "w", "data": {"type": "text"}}]
        msgs_m2 = [{"id": "msg_wedged2", "time": very_old_m,
                    "time_data": {"created": very_old_m}, "role": "assistant"}]
        st_m2 = {"last_evaluated_mid": "msg_wedged2", "last_injected_at": 0}
        evaluate.__globals__["llama_idle"] = (lambda c, s, w=None: False)  # busy
        action_m4, reason_m4, _h = evaluate(cfg_m, {"id": "s", "title": "t"},
                                            parts_m2, msgs_m2, st_m2)
        ok_m = ok_m and action_m4 == "nothing"  # stand down while llama busy
    finally:
        evaluate.__globals__["llama_idle"] = orig_ml
        shutil.rmtree(d_m, ignore_errors=True)
    results.append(("stall_retrigger", "pass", "pass", ok_m,
                    "wedged turn re-triggers; throttle cadence; llama stand-down", []))
    print("%-24s expect=%-8s got=%-8s %s  %s" % (
        "stall_retrigger", "pass", "pass", "OK" if ok_m else "MISMATCH",
        "wedged re-trigger cadence; throttle cadence; llama stand-down"))

    # L: real llama_idle ORs CPU fraction with llama-server slots is_processing
    # (heavy-GPU-offload case: CPU quiet while a slot is still decoding) and
    # with the decoded/ingested token speeds. GPU utilisation is a fallback
    # only (see L2) because it is machine-wide, not per-process.
    g = llama_idle.__globals__
    def mkllama_idle_test(cpu_frac, slots_processing, gate_slots,
                          gpu_max=0.0, gate_gpu=True, gate_threshold=10.0,
                          token_speeds=(0.0, 0.0), gate_tokens=True):
        o_frac = g["llama_cpu_frac"]
        o_slots = g["llama_slots_processing"]
        o_gpu = g["llama_gpu_util"]
        o_tok = g["llama_token_speeds"]
        cfg = dict(CONFIG_DEFAULTS)
        cfg["stall_gate_slots"] = gate_slots
        cfg["stall_gate_gpu"] = gate_gpu
        cfg["stall_gate_tokens"] = gate_tokens
        cfg["stall_llama_gpu_util"] = gate_threshold
        try:
            g["llama_cpu_frac"] = lambda c, s, w=None: cpu_frac
            g["llama_slots_processing"] = (lambda c: True) if slots_processing \
                else (lambda c: False)
            g["llama_gpu_util"] = lambda c, s=None: {"sum": float(gpu_max),
                                                    "max": float(gpu_max)}
            g["llama_token_speeds"] = lambda c: tuple(token_speeds)
            return llama_idle(cfg, {}, None)
        finally:
            g["llama_cpu_frac"] = o_frac
            g["llama_slots_processing"] = o_slots
            g["llama_gpu_util"] = o_gpu
            g["llama_token_speeds"] = o_tok
    ok_l1 = mkllama_idle_test(0.5, False, True) is False   # CPU active -> not idle
    ok_l2 = mkllama_idle_test(0.0, True, True) is False    # CPU idle, slot decoding -> not idle
    ok_l3 = mkllama_idle_test(0.0, False, True) is True    # CPU idle, slot idle -> idle
    ok_l4 = mkllama_idle_test(0.0, True, False) is True    # slots gate disabled -> idle
    ok_l5 = mkllama_idle_test(0.5, True, True) is False    # both signals -> not idle
    ok_l6 = mkllama_idle_test(0.0, False, True, token_speeds=(30.0, 0.0)) is False   # decoding -> not idle
    ok_l7 = mkllama_idle_test(0.0, False, True, token_speeds=(0.0, 120.0)) is False  # ingesting -> not idle
    ok_l8 = mkllama_idle_test(0.0, False, True, token_speeds=(30.0, 0.0), gate_tokens=False) is True  # token gate disabled -> idle
    results.append(("llama_idle_slots", "pass", "pass",
                    all([ok_l1, ok_l2, ok_l3, ok_l4, ok_l5, ok_l6, ok_l7, ok_l8]),
                    "llama_idle ORs CPU frac + slots is_processing + token speeds", []))
    print("%-22s expect=%-9s got=%-9s %s  %s" % (
        "llama_idle_slots", "pass", "pass",
        "OK" if all([ok_l1, ok_l2, ok_l3, ok_l4, ok_l5, ok_l6, ok_l7, ok_l8]) else "MISMATCH",
        "llama_idle ORs CPU frac + slots is_processing + token speeds"))

    # L2: the machine-wide GPU reading is a FALLBACK, not an independent veto.
    # With the direct slots/tokens signals enabled (default) it is ignored —
    # desktop/compositor noise at ~10-15% must not veto idleness (2026-09-22
    # false-busy, 20 min blocked). It only vetoes when BOTH direct gates are
    # disabled (server builds exposing neither slots nor token counters).
    ok_g1 = mkllama_idle_test(0.0, False, True, gpu_max=40.0) is True    # direct gates on: GPU noise ignored
    ok_g2 = mkllama_idle_test(0.0, False, True, gpu_max=5.0) is True     # quiet GPU -> idle
    ok_g3 = mkllama_idle_test(0.0, False, True, gpu_max=40.0, gate_gpu=False) is True  # GPU gate off -> idle
    ok_g4 = mkllama_idle_test(0.5, False, True, gpu_max=40.0) is False   # CPU -> not idle
    ok_g5 = mkllama_idle_test(0.0, False, False, gpu_max=40.0,
                              gate_tokens=False) is False  # fallback busy
    ok_g6 = mkllama_idle_test(0.0, False, False, gpu_max=2.0,
                              gate_tokens=False) is True   # fallback idle
    results.append(("llama_idle_gpu", "pass", "pass",
                    all([ok_g1, ok_g2, ok_g3, ok_g4, ok_g5, ok_g6]),
                    "GPU reading is a fallback veto only when slots+tokens gates are disabled", []))
    print("%-22s expect=%-9s got=%-9s %s  %s" % (
        "llama_idle_gpu", "pass", "pass",
        "OK" if all([ok_g1, ok_g2, ok_g3, ok_g4, ok_g5, ok_g6]) else "MISMATCH",
        "GPU reading is a fallback veto only when slots+tokens gates are disabled"))

    # N2: multi-session config/state plumbing
    ok_ms1 = monitored_roots(
        {"monitored_sessions": {"a": {"enabled": True}, "b": {"enabled": False},
                                "c": {}}}) == ["a"]
    ok_ms2 = monitored_roots({}) == []
    st_ms = {}
    per_ms = per_state(st_ms, "ses_x")
    ok_ms3 = per_ms["continues"] == 0 and "last_evaluated_mid" in per_ms
    ok_ms4 = st_ms["per"]["ses_x"] is per_ms and per_state(st_ms, "ses_y") is not None
    results.append(("multi_config_state", "pass", "pass",
                    all([ok_ms1, ok_ms2, ok_ms3, ok_ms4]),
                    "monitored_roots + per_state defaults", []))
    print("%-24s expect=%-8s got=%-8s %s  %s" % (
        "multi_config_state", "pass", "pass",
        "OK" if all([ok_ms1, ok_ms2, ok_ms3, ok_ms4]) else "MISMATCH",
        "monitored_roots + per_state defaults"))

    # T: session tree parent_id leaf resolution + transcript growth
    def _c_fake(parts_after):
        return [{"time": 2}, {"time": parts_after}]

    tmpdb = Path(tempfile.mkdtemp()) / "opencode.db"
    _c = sqlite3.connect(str(tmpdb))
    _c.executescript(
        "CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, title TEXT, "
        "model TEXT, directory TEXT, time_updated INTEGER);"
        "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, "
        "time_updated INTEGER, data TEXT);"
        "CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, "
        "time_created INTEGER, time_updated INTEGER, data TEXT);")
    _c.execute("INSERT INTO session VALUES ('root','','Root','{}','C:/r',1)")
    _c.execute("INSERT INTO session VALUES ('child','root','Child','{}','C:/r',2)")
    _c.execute("INSERT INTO session VALUES ('done','child','DoneChild','{}','C:/r',3)")
    _c.execute("INSERT INTO message VALUES ('m1','root',1,10,'{\"role\":\"assistant\",\"time\":{},\"modelID\":\"m\"}')")
    _c.execute("INSERT INTO message VALUES ('m2','child',2,20,'{\"role\":\"assistant\",\"time\":{},\"modelID\":\"m\"}')")
    _c.execute("INSERT INTO message VALUES ('m3','done',3,30,'{\"role\":\"assistant\",\"time\":{\"completed\":1},\"modelID\":\"m\"}')")
    _c.execute("INSERT INTO part VALUES ('p1','m1','root',1,1,'{\"type\":\"text\",\"text\":\"hi\"}')")
    _c.execute("INSERT INTO part VALUES ('p2','m2','child',2,2,'{\"type\":\"text\",\"text\":\"hi\"}')")
    _c.execute("INSERT INTO part VALUES ('p3','m3','done',3,3,'{\"type\":\"text\",\"text\":\"hi\"}')")
    _c.commit()
    _c.close()

    orig_db = db_path.__globals__["db_path"]
    ok_t = False
    try:
        db_path.__globals__["db_path"] = lambda: tmpdb
        with connect_db(tmpdb) as _con:
            root = session_row(_con, "root")
            leaf = active_leaf(_con, root)
            # deepest live child ('done' has a completed message -> skip it)
            ok_t1 = leaf["id"] == "child"
            per_g = {"last_seen_at": 1}
            ok_t2 = node_growth(_c_fake(parts_after=2), per_g)
            ok_t3 = not node_growth([], per_g)
        ok_t = all([ok_t1, ok_t2, ok_t3])
    finally:
        db_path.__globals__["db_path"] = orig_db
        shutil.rmtree(tmpdb.parent, ignore_errors=True)
    results.append(("multi_session_tree", "pass", "pass", ok_t,
                    "parent_id leaf resolution + growth", []))
    print("%-24s expect=%-8s got=%-8s %s  %s" % (
        "multi_session_tree", "pass", "pass",
        "OK" if ok_t else "MISMATCH",
        "parent_id leaf resolution + growth"))

    # M: escalation note round-trips into a temp session DB (never the live DB)
    tmpdb = Path(tempfile.gettempdir()) / ("hawk_selftest_%s.db" % uuid.uuid4().hex[:8])
    note_text = "TEST ESCALATION NOTE %s" % uuid.uuid4().hex[:8]
    c0 = sqlite3.connect(str(tmpdb))
    c0.execute("CREATE TABLE message (id text PRIMARY KEY, session_id text NOT NULL, "
               "time_created integer NOT NULL, time_updated integer NOT NULL, data text NOT NULL)")
    c0.execute("CREATE TABLE part (id text PRIMARY KEY, message_id text NOT NULL, "
               "session_id text NOT NULL, time_created integer NOT NULL, "
               "time_updated integer NOT NULL, data text NOT NULL)")
    c0.commit()
    c0.close()
    try:
        wrote = append_session_note(
            tmpdb, "ses_test", note_text,
            {"providerID": "llama.cpp", "modelID": "qwen3"})
        c1 = connect_db(tmpdb)
        msgs = session_messages(c1, "ses_test")
        parts = session_parts(c1, "ses_test")
        c1.close()
        read_role = any(m.get("role") == "user" for m in msgs)
        read_text = any(note_text in (p.get("text") or "") for p in parts)
        bad_path = tmpdb.parent / "no_such_dir_must_fail" / "x.db"
        bad = append_session_note(bad_path, "ses_test", "x",
                                  {"providerID": "llama.cpp"})
        ok_m = (wrote is True and read_role and read_text and bad is False)
    finally:
        try:
            tmpdb.unlink()
        except Exception:
            pass
    results.append(("escalation_note_db", "pass", "pass", ok_m,
                    "escalation note written to session DB as synthetic user msg + text part", []))
    print("%-22s expect=%-9s got=%-9s %s  %s" % (
        "escalation_note_db", "pass", "pass",
        "OK" if ok_m else "MISMATCH",
        "escalation note written to session DB as synthetic user msg + text part"))

    # N: --auto present on both continue paths (attach + standalone)
    cli_dummy = "opencode"
    cmd_att = build_continue_cmd(cli_dummy, "http://127.0.0.1:1", "ses_x", "/proj", "MSG")
    cmd_st = build_continue_cmd(cli_dummy, None, "ses_x", "/proj", "MSG")
    ok_n = ("--auto" in cmd_att and "--auto" in cmd_st
            and "--attach" in cmd_att and "http://127.0.0.1:1" in cmd_att
            and "--session" in cmd_att and "ses_x" in cmd_att
            and cmd_att[-1] == "MSG" and cmd_st[-1] == "MSG")
    results.append(("continue_auto_flag", "pass", "pass", ok_n,
                    "--auto on both attach and standalone continue commands", []))
    print("%-22s expect=%-9s got=%-9s %s  %s" % (
        "continue_auto_flag", "pass", "pass",
        "OK" if ok_n else "MISMATCH",
        "--auto on both attach and standalone continue commands"))

    # O: select_session auto-follows the newer project-dir session when the
    # configured one has gone idle, and persists the migration to config.json.
    tmpdb = Path(tempfile.gettempdir()) / ("hawk_selftest_%s.db" % uuid.uuid4().hex[:8])
    d_home = Path(tempfile.mkdtemp())
    c0 = sqlite3.connect(str(tmpdb))
    c0.execute("CREATE TABLE session (id text PRIMARY KEY, title text, model text, "
               "directory text, time_updated integer NOT NULL)")
    c0.execute("CREATE TABLE message (id text PRIMARY KEY, session_id text NOT NULL, "
               "time_created integer NOT NULL, time_updated integer NOT NULL, data text NOT NULL)")
    old_ms = 1_000_000_000
    new_ms = int(time.time() * 1000) + 1_000_000
    c0.execute("INSERT INTO session VALUES (?,?,?,?,?)",
               ("ses_config", "idle config session", "{}", "C:/Repo", old_ms))
    c0.execute("INSERT INTO message VALUES (?,?,?,?,?)",
               ("m_c1", "ses_config", old_ms, old_ms, "{}"))
    c0.execute("INSERT INTO session VALUES (?,?,?,?,?)",
               ("ses_live", "live worker session", "{}", "C:/Repo", new_ms))
    c0.execute("INSERT INTO message VALUES (?,?,?,?,?)",
               ("m_ll", "ses_live", new_ms, new_ms, "{}"))
    c0.commit()
    c0.close()
    cfg_o = {"project_dir": "C:/Repo", "session_id": "ses_config", "idle_minutes": 8}
    ok_o = False
    try:
        old_home = os.environ.get("COORD_HOME")
        os.environ["COORD_HOME"] = str(d_home)
        try:
            con_o = sqlite3.connect(str(tmpdb))
            got = select_session(con_o, cfg_o)
            con_o.close()
            persisted = json.loads((d_home / "config.json").read_text(encoding="utf-8"))
            ok_o = (got is not None and got["id"] == "ses_live"
                    and cfg_o["session_id"] == "ses_live"
                    and persisted.get("session_id") == "ses_live")
        finally:
            if old_home is not None:
                os.environ["COORD_HOME"] = old_home
            else:
                os.environ.pop("COORD_HOME", None)
    finally:
        try:
            tmpdb.unlink()
        except Exception:
            pass
    shutil.rmtree(d_home, ignore_errors=True)
    results.append(("session_follow", "pass", "pass", ok_o,
                     "select_session migrates an idle configured session to the newer project-dir session", []))
    print("%-22s expect=%-9s got=%-9s %s  %s" % (
        "session_follow", "pass", "pass",
        "OK" if ok_o else "MISMATCH",
        "select_session migrates an idle configured session to the newer project-dir session"))

    # single-session gate: a session listed in monitored_sessions with
    # enabled=false must NOT fall through to poll_once; absent entry keeps
    # legacy behaviour; --session-id CLI override bypasses the checkbox.
    ok_ss = (
        _single_session_enabled({"session_id": "ses_a",
                                 "monitored_sessions": {"ses_a": {"enabled": False}}}, False) is False
        and _single_session_enabled({"session_id": "ses_a",
                                     "monitored_sessions": {"ses_a": {"enabled": True}}}, False) is True
        and _single_session_enabled({"session_id": "ses_a"}, False) is True
        and _single_session_enabled({"session_id": "ses_a",
                                     "monitored_sessions": {"ses_a": {"enabled": False}}}, True) is True
        and _single_session_enabled({"session_id": ""}, False) is False
    )
    results.append(("single_session_gate", "pass", "pass", ok_ss,
                    "disabled monitored session is never auto-continued; CLI --session-id still works", []))
    print("%-22s expect=%-9s got=%-9s %s  %s" % (
        "single_session_gate", "pass", "pass",
        "OK" if ok_ss else "MISMATCH",
        "disabled monitored session is never auto-continued; CLI --session-id still works"))

    # apply_action: default gating preserves single-session behavior; auto flag
    ga = apply_action.__globals__
    o_send = ga["send_continue"]
    o_save = ga["save_state"]
    a_calls = []
    def a_fake_send(cfg, s, p, m, auto=True):
        a_calls.append((s, p, m, auto))
        return True
    ga["send_continue"] = a_fake_send
    ga["save_state"] = lambda st: None
    st_blk = {"continues": 0}
    ok_a = False
    try:
        r = apply_action(dict(CONFIG_DEFAULTS), st_blk, {"id": "sx", "title": "t"},
                         "C:/p", [], [{"id": "m1"}], "continue", "", [], auto=True)
        ok_a1 = r == 3 and a_calls[-1][3] is True
        r2 = apply_action(dict(CONFIG_DEFAULTS), st_blk, {"id": "sx", "title": "t"},
                          "C:/p", [], [{"id": "m1"}], "continue", "", [], dry_run=True)
        ok_a2 = r2 == 0
        r3 = apply_action(dict(CONFIG_DEFAULTS), st_blk, {"id": "sx", "title": "t"},
                          "C:/p", [], [{"id": "m1"}], "continue", "", [], enable_continue=False)
        ok_a3 = r3 == 0
        ok_a = all([ok_a1, ok_a2, ok_a3])
    finally:
        ga["send_continue"] = o_send
        ga["save_state"] = o_save
    results.append(("apply_action_gating", "pass", "pass", ok_a,
                    "apply_action defaults/auto/suppression", []))
    print("%-24s expect=%-8s got=%-8s %s  %s" % (
        "apply_action_gating", "pass", "pass",
        "OK" if ok_a else "MISMATCH", "apply_action defaults/auto/suppression"))

    # apply_action continue delivery failure: must NOT advance last_evaluated_mid
    # or continues (next poll retries), and must write a throttled escalation.
    ga = apply_action.__globals__
    o_send2 = ga["send_continue"]
    o_save2 = ga["save_state"]
    o_an2 = ga["analyze"]
    o_esc2 = ga["write_escalation"]
    esc_calls = []
    ga["send_continue"] = lambda *a, **k: False
    ga["save_state"] = lambda st: None
    ga["analyze"] = lambda pts, msgs: {"last_assistant": "x", "tool_blob": "y"}
    ga["write_escalation"] = lambda *a, **k: (esc_calls.append(a) or "C:/tmp/esc.md")
    st_fail = {"continues": 0}
    ok_f = False
    try:
        r1 = apply_action(dict(CONFIG_DEFAULTS), st_fail, {"id": "sx", "title": "t"},
                          "C:/p", [], [{"id": "m1"}], "continue", "the reason", [],
                          auto=True)
        after1 = dict(st_fail)
        ok_f1 = (r1 == 2 and "last_evaluated_mid" not in st_fail
                 and st_fail.get("continues", 0) == 0
                 and len(esc_calls) == 1
                 and after1.get("last_continue_fail_at"))
        r2 = apply_action(dict(CONFIG_DEFAULTS), st_fail, {"id": "sx", "title": "t"},
                          "C:/p", [], [{"id": "m1"}], "continue", "", [],
                          auto=True)
        ok_f2 = r2 == 2 and len(esc_calls) == 1 and "last_evaluated_mid" not in st_fail
        st_fail["last_continue_fail_at"] = int(time.time() * 1000) - 3600_000
        r3 = apply_action(dict(CONFIG_DEFAULTS), st_fail, {"id": "sx", "title": "t"},
                          "C:/p", [], [{"id": "m1"}], "continue", "", [],
                          auto=True)
        ok_f3 = r3 == 2 and len(esc_calls) == 2 and "last_evaluated_mid" not in st_fail
        ga["send_continue"] = lambda *a, **k: True
        r4 = apply_action(dict(CONFIG_DEFAULTS), st_fail, {"id": "sx", "title": "t"},
                          "C:/p", [], [{"id": "m1"}], "continue", "", [],
                          auto=True)
        ok_f4 = (r4 == 3 and st_fail.get("last_evaluated_mid") == "m1"
                 and st_fail.get("continues") == 1
                 and "last_continue_fail_at" not in st_fail)
        ok_f = all([ok_f1, ok_f2, ok_f3, ok_f4])
    finally:
        ga["send_continue"] = o_send2
        ga["save_state"] = o_save2
        ga["analyze"] = o_an2
        ga["write_escalation"] = o_esc2
    results.append(("apply_action_continue_fail", "pass", "pass", ok_f,
                    "continue delivery failure: no mid advance, throttled escalation, retry", []))
    print("%-25s expect=%-8s got=%-8s %s  %s" % (
        "apply_action_continue_fail", "pass", "pass",
        "OK" if ok_f else "MISMATCH",
        "continue delivery failure: no mid advance, throttled escalation, retry"))

    # MP: multi-session poll loop picks the most-recently-active leaf; stands
    # down while anything is producing; returns 0 with no candidates.
    mdb = Path(tempfile.mkdtemp()) / "opencode.db"
    _mc = sqlite3.connect(str(mdb))
    _mc.executescript(
        "CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, title TEXT, "
        "model TEXT, directory TEXT, time_updated INTEGER);"
        "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, "
        "time_updated INTEGER, data TEXT);"
        "CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, "
        "time_created INTEGER, time_updated INTEGER, data TEXT);"
        "CREATE TABLE todo (session_id TEXT, position INTEGER, content TEXT, "
        "status TEXT, priority TEXT, time_created INTEGER, time_updated INTEGER);")
    _mc.executemany(
        "INSERT INTO session VALUES (?,?,?,?,?,?)",
        [("a", None, "A", "{}", "C:/pa", 1000),
         ("b", None, "B", "{}", "C:/pb", 9000),
         ("b1", "b", "B1", "{}", "C:/pb", 9050)])
    _mc.executemany(
        "INSERT INTO message VALUES (?,?,?,?,?)",
        [("ma1", "a", 500, 1000, '{"role":"assistant","time":{},"modelID":"m"}'),
         ("mb1", "b", 500, 9000, '{"role":"assistant","time":{},"modelID":"m"}'),
         ("mb11", "b1", 9000, 9050, '{"role":"assistant","time":{},"modelID":"m"}')])
    _mc.executemany(
        "INSERT INTO part VALUES (?,?,?,?,?,?)",
        [("pa1", "ma1", "a", 500, 1000, '{"type":"text","text":"hi"}'),
         ("pb1", "mb1", "b", 500, 9000, '{"type":"text","text":"hi"}'),
         ("pb1x", "mb11", "b1", 9000, 9050, '{"type":"text","text":"hi"}')])
    _mc.commit()
    _mc.close()

    def _mk_mscfg(ms):
        c = dict(mkcfg(CONFIG_DEFAULTS))
        c["monitored_sessions"] = ms
        c["idle_minutes"] = 30
        return c

    g_aa = poll_multisession.__globals__
    o_aa = g_aa["apply_action"]
    o_save = g_aa["save_state"]
    orig_mp = db_path.__globals__["db_path"]
    ok_mp = False
    try:
        def fake_apply_action(cfg, state, session, project_dir, parts, messages,
                              action, reason, rule_hits, dry_run=False,
                              enable_continue=True, auto=True,
                              node_sid=None, node_per=None):
            calls.append(node_sid)
            return 3
        g_aa["apply_action"] = fake_apply_action
        g_aa["save_state"] = lambda st: None
        db_path.__globals__["db_path"] = lambda: mdb
        calls = []
        # Fresh per-session state primes its baseline and defers evaluation
        # one pass (same contract poll_once uses); pick logic runs only on
        # sessions whose baseline is already recorded.
        st0 = {"per": {}}
        r0 = poll_multisession(_mk_mscfg({"a": {"enabled": True}}), st0)
        ok_mp0 = r0 == 0 and calls == [] and "prev_head" in st0["per"].get("a", {})
        st_ms = {"per": {}}
        per_state(st_ms, "a")["last_seen_at"] = 600
        per_state(st_ms, "b")["last_seen_at"] = 600
        per_state(st_ms, "b1")["last_seen_at"] = 9600
        per_state(st_ms, "a")["prev_head"] = "h"
        per_state(st_ms, "b")["prev_head"] = "h"
        r1 = poll_multisession(_mk_mscfg(
            {"a": {"enabled": True}, "b": {"enabled": True}}), st_ms)
        ok_mp1 = r1 == 3 and calls == ["b"]
        _mc = sqlite3.connect(str(mdb))
        _mc.execute("UPDATE part SET time_updated=?, time_created=? WHERE id='pa1'",
                    (int(time.time() * 1000), int(time.time() * 1000)))
        _mc.commit()
        _mc.close()
        r2 = poll_multisession(_mk_mscfg(
            {"a": {"enabled": True}, "b": {"enabled": True}}), st_ms)
        ok_mp2 = r2 == 0 and calls == ["b"]
        st_ms2 = {"per": {}}
        per_state(st_ms2, "a")["last_evaluated_mid"] = "ma1"
        per_state(st_ms2, "b")["last_evaluated_mid"] = "mb11"
        per_state(st_ms2, "a")["prev_head"] = "h"
        per_state(st_ms2, "b")["prev_head"] = "h"
        r3 = poll_multisession(_mk_mscfg(
            {"a": {"enabled": True}, "b": {"enabled": True}}), st_ms2)
        # st_ms2: neither session grew a new message, but session b has an
        # unfinished turn (no completed in time_data) wedged long past
        # stalled_turn_minutes -> it is a stalled candidate and gets the
        # self-check; a's recent part keeps it out.
        ok_mp3 = r3 == 3 and calls[-1] == "b"
        ok_mp = all([ok_mp0, ok_mp1, ok_mp2, ok_mp3])
    finally:
        g_aa["apply_action"] = o_aa
        g_aa["save_state"] = o_save
        db_path.__globals__["db_path"] = orig_mp
        shutil.rmtree(mdb.parent, ignore_errors=True)
    results.append(("multi_poll_loop", "pass", "pass", ok_mp,
                    "poll_multisession pick/stand-down/stalled-candidate", []))
    print("%-24s expect=%-8s got=%-8s %s  %s" % (
        "multi_poll_loop", "pass", "pass",
        "OK" if ok_mp else "MISMATCH",
        "poll_multisession pick/stand-down/stalled-candidate"))

    # AB: a non-git project dir converges after ONE priming pass (prev
    #     used to sit at "" and re-prime every pass, skipping evaluate).
    #     On the next pass, the completed turn at rest past idle reaches
    #     the stall self-check.
    ok_ab = False
    g_ab = poll_multisession.__globals__
    o_aa_ab = g_ab["apply_action"]
    o_save_ab = g_ab["save_state"]
    orig_mp_ab = db_path.__globals__["db_path"]
    try:
        nongit = tempfile.mkdtemp()
        mdb2 = Path(tempfile.mkdtemp()) / "opencode.db"
        _mc = sqlite3.connect(str(mdb2))
        _mc.executescript(
            "CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, title TEXT, "
            "model TEXT, directory TEXT, time_updated INTEGER);"
            "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, "
            "time_updated INTEGER, data TEXT);"
            "CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, "
            "time_created INTEGER, time_updated INTEGER, data TEXT);"
            "CREATE TABLE todo (session_id TEXT, position INTEGER, content TEXT, "
            "status TEXT, priority TEXT, time_created INTEGER, time_updated INTEGER);")
        old_t = int(time.time() * 1000) - 2 * 60 * 60 * 1000
        _mc.execute("INSERT INTO session VALUES (?,?,?,?,?,?)",
                    ("a", None, "A", "{}", nongit, 1000))
        _mc.execute("INSERT INTO message VALUES (?,?,?,?,?)",
                    ("ma1", "a", old_t, old_t,
                     '{"role":"assistant","time":{"created":%d,"completed":%d},'
                     '"modelID":"m"}' % (old_t, old_t)))
        _mc.execute("INSERT INTO part VALUES (?,?,?,?,?,?)",
                    ("pa1", "ma1", "a", old_t, old_t,
                     '{"type":"text","text":"Which do you want? If none, '
                     'consider it closed."}'))
        _mc.commit()
        _mc.close()
        calls_ab = []
        def fake_apply_action_ab(cfg, state, session, project_dir, parts,
                                 messages, action, reason, rule_hits,
                                 dry_run=False, enable_continue=True,
                                 auto=True, node_sid=None, node_per=None):
            calls_ab.append(node_sid)
            return 3
        g_ab["apply_action"] = fake_apply_action_ab
        g_ab["save_state"] = lambda st: None
        db_path.__globals__["db_path"] = lambda: mdb2
        orig_idle_ab = evaluate.__globals__["llama_idle"]
        try:
            evaluate.__globals__["llama_idle"] = (lambda c, s, w=None: True)
            st_ab = {"per": {}}
            r1_ab = poll_multisession(_mk_mscfg({"a": {"enabled": True}}), st_ab)
            primed_ok = (r1_ab == 0 and calls_ab == []
                         and st_ab["per"]["a"].get("primed") is True
                         and st_ab["per"]["a"].get("prev_head") == ""
                         and st_ab["per"]["a"].get("last_evaluated_mid") == "ma1")
            r2_ab = poll_multisession(_mk_mscfg({"a": {"enabled": True}}), st_ab)
            ok_ab = primed_ok and r2_ab == 3 and calls_ab == ["a"]
        finally:
            evaluate.__globals__["llama_idle"] = orig_idle_ab
    finally:
        g_ab["apply_action"] = o_aa_ab
        g_ab["save_state"] = o_save_ab
        db_path.__globals__["db_path"] = orig_mp_ab
        shutil.rmtree(mdb2.parent, ignore_errors=True)
        shutil.rmtree(nongit, ignore_errors=True)
    results.append(("non_git_prime_converges", "pass", "pass", ok_ab,
                    "non-git dir: one priming pass, then stall self-check", []))
    print("%-24s expect=%-8s got=%-8s %s  %s" % (
        "non_git_prime_converges", "pass", "pass",
        "OK" if ok_ab else "MISMATCH",
        "non-git dir: one priming pass, then stall self-check"))

    # AB2: re-stall cadence on a completed turn that keeps "stopping": a
    #     self-check injected recently -> nothing (inside re_stall window);
    #     window elapsed -> continue again.
    parts, msgs = mkparts("Which do you want? If none, consider it closed.",
                          last_age_s=3600)
    run("stalled_cadence_blocked", "nothing", parts, msgs,
        {"last_evaluated_mid": "msg_test_final",
         "last_injected_at": int(time.time() * 1000) - 10 * 60 * 1000})
    parts, msgs = mkparts("Which do you want? If none, consider it closed.",
                          last_age_s=3600)
    run("stalled_cadence_elapsed", "continue", parts, msgs,
        {"last_evaluated_mid": "msg_test_final",
         "last_injected_at": int(time.time() * 1000) - 60 * 60 * 1000})

    # PA: extract_choices (shared with notify.py for mobile buttons)
    ok_aa1 = extract_choices("Pick one:\n1. green\n2. blue") == ["green", "blue"]
    ok_aa2 = extract_choices("No list here.") == []
    ok_aa = all([ok_aa1, ok_aa2])
    results.append(("extract_choices", "pass", "pass", ok_aa,
                    "extract_choices parses numbered/bulleted options", []))
    print("%-24s expect=%-8s got=%-8s %s  %s" % (
        "extract_choices", "pass", "pass",
        "OK" if ok_aa else "MISMATCH",
        "extract_choices parses numbered/bulleted options"))

    # run_tests: project-aware runner selection + cargo lock-retry + Python
    # dashboard selftest parse (all offline; subprocess.run is faked).
    rt = Path(tempfile.mkdtemp())
    (rt / "Cargo.toml").write_text("[package]\nname = 'x'\nversion = '0.1.0'\n",
                                   encoding="utf-8")
    ok_rc = project_test_command(str(rt)) == (["cargo", "test"], "cargo")
    (rt / "Cargo.toml").unlink()
    (rt / "dashboard.py").write_text("", encoding="utf-8")
    ok_rs = project_test_command(str(rt))[1] == "selftest"
    ok_rn = project_test_command(str(tempfile.mkdtemp())) == (None, None)
    rtmp2 = Path(tempfile.mkdtemp())
    (rtmp2 / "Cargo.toml").write_text("", encoding="utf-8")
    real_run = subprocess.run
    real_sleep = time.sleep
    try:
        seq = [
            subprocess.CompletedProcess(
                [], 0, "",
                "Blocking waiting for file lock on build directory\n"),
            subprocess.CompletedProcess(
                [], 0,
                "test result: ok. 3 passed; 0 failed; 0 ignored; finished in 0.50s\n",
                ""),
        ]
        subprocess.run = lambda *a, **k: seq.pop(0)
        time.sleep = lambda s: None
        t_lock = run_tests(str(rtmp2), 300, max_attempts=2, lock_wait_s=1)
        ok_lock = t_lock["ok"] is True and t_lock["passed"] == 3
        seq3 = [subprocess.CompletedProcess(
            [], 0, "", "Blocking waiting for file lock on build directory\n")]
        subprocess.run = lambda *a, **k: seq3[0]
        t_exh = run_tests(str(rtmp2), 300, max_attempts=2, lock_wait_s=1)
        ok_exh = t_exh["ok"] is False and t_exh["note"] == "unparseable result"
        subprocess.run = lambda *a, **k: subprocess.CompletedProcess(
            [], 0, "dashboard self-test: PASS\n", "")
        t_self = run_tests(str(rt), 300)
        ok_self = t_self["ok"] is True and t_self["note"] == "selftest PASS"
        subprocess.run = lambda *a, **k: subprocess.CompletedProcess(
            [], 1, "FAIL\n", "dashboard self-test: FAIL\n")
        t_selffail = run_tests(str(rt), 300)
        ok_selffail = t_selffail["ok"] is False and t_selffail["note"] == "selftest FAIL"
    finally:
        subprocess.run = real_run
        time.sleep = real_sleep
        shutil.rmtree(rt, ignore_errors=True)
        shutil.rmtree(rtmp2, ignore_errors=True)
    ok_runner = all([ok_rc, ok_rs, ok_rn, ok_lock, ok_exh, ok_self, ok_selffail])
    results.append(("runner_project_aware", "pass", "pass", ok_runner,
                    "run_tests picks the tree's own runner; cargo gate retries on lock", []))

    # One fake opencode.db builder for every permission test below. It creates
    # ONLY `session`: the reworked discovery reads that table and nothing else
    # (session_row for `directory`, plus perm_root_of's raw parent_id walk), and
    # the active_leaf/session_messages callers are all steering, which these
    # hermetic sweeps never reach. The column set is the one the live DB has and
    # the one all three previous inline fixtures shared. `title`/`model` are
    # filler that the permission path never reads: session_row selects them
    # because it must match the live query, and nothing here looks at them.
    def _fake_session_db(rows):
        """rows: (id, parent_id, directory) -> (tmpdir, db path)."""
        d = Path(tempfile.mkdtemp())
        p = d / "opencode.db"
        con = sqlite3.connect(str(p))
        con.execute("CREATE TABLE session (id TEXT PRIMARY KEY, title TEXT, model TEXT, "
                    "directory TEXT, parent_id TEXT, time_updated INTEGER)")
        now = int(time.time() * 1000)
        for i, (sid, pid, dr) in enumerate(rows):
            con.execute("INSERT INTO session VALUES (?,?,?,?,?,?)", (sid, "t", "m", dr, pid, now + i))
        con.commit()
        con.close()
        return d, p

    # Desktop permission auto-accept. Fixtures are the *captured* payloads from a
    # live sidecar (see plan): the GUI's file-location asks arrive on the v1
    # route with the field named "permission", while the CLI/v2 route names it
    # "action" -- an earlier hand-written v2-only fixture kept this test green
    # while the feature was dead. Only external_directory is replied, dedupe
    # stops re-replies, an absent route (403 HTML from the app.opencode.ai
    # passthrough) is not mistaken for an empty queue, and the config flag
    #   disables the whole sweep. All offline; desktop_sidecar/_perm_request faked.
    psweep_g = desktop_permission_sweep.__globals__
    o_sidecar = psweep_g["desktop_sidecar"]
    o_permreq = psweep_g["_perm_request"]
    o_replied = psweep_g["_PERM_REPLIED"]
    o_home = psweep_g["home_dir"]
    _perm_tdir = Path(tempfile.mkdtemp())
    psweep_g["home_dir"] = lambda: _perm_tdir
    perm_calls = []
    psweep_g["desktop_sidecar"] = lambda: (
        "http://127.0.0.1:9999", {"username": "opencode", "password": "s3cret"})
    _hs_dir = r"C:\Users\dev\Desktop\demo_project"
    _co_dir = r"C:\Users\dev\coordinator"
    _hs_q = urllib.parse.quote(_hs_dir, safe="")
    _co_q = urllib.parse.quote(_co_dir, safe="")
    _ov_dir = r"C:\Users\dev\AppData\Local\Temp\ov"
    _cf_403 = b"<!doctype html>\n<html><head><title>Access denied | Cloudflare</title>"
    # Shape a *registered* session gets. `enabled` stays False throughout: the
    # permission filter is independent of hawk's steering toggle, so a disabled
    # session's ask must still be auto-accepted.
    _perm_reg = {"enabled": False, "auto_accept": True, "auto_continue": False,
                 "project_dir": ""}
    def _fake_perm(base, creds, path, method="GET", body=None, timeout=8):
        if path.endswith("/reply") or "/reply?" in path:
            perm_calls.append((path, method, body))
            return (200, b"true") if path.startswith("/permission/") else (204, b"")
        return {
            "/project": (200, json.dumps([
                {"id": "global", "worktree": "/"},
                {"id": "df32b552", "worktree": _hs_dir},
                {"id": "db29a534", "worktree": _co_dir},
            ]).encode()),
            # v1: verbatim record captured from the Desktop GUI ask
            "/permission?directory=" + _hs_q: (200, json.dumps([
                {"id": "per_08242587e00102p72sl9a1pUUb",
                 "sessionID": "ses_f91e010d7ffegdVQFmfTdOOBsU",
                 "permission": "external_directory",
                 "patterns": [_ov_dir + r"\*"],
                 "always": [_ov_dir + r"\*"],
                 "metadata": {"filepath": _ov_dir + r"\risky.py",
                              "parentDir": _ov_dir}},
                {"id": "per_v1_bash", "sessionID": "ses_f91e010d7ffegdVQFmfTdOOBsU",
                 "permission": "bash", "patterns": ["rm -rf *"]},
            ]).encode()),
            "/permission?directory=" + _co_q: (200, b"[]"),
            "/api/permission/request?location%5Bdirectory%5D=" + _co_q: (200, json.dumps({
                "location": {"directory": _co_dir}, "data": [
                    {"id": "per_v2_dir", "sessionID": "ses_b", "action": "external_directory",
                     "resources": [r"C:\Users\dev\Documents\x\**"]},
                    {"id": "per_v2_bash", "sessionID": "ses_b", "action": "bash",
                     "resources": ["rm -rf *"]},
                ]}).encode()),
        }.get(path, (403, _cf_403))  # everything else: the Cloudflare passthrough
    psweep_g["_perm_request"] = _fake_perm
    try:
        psweep_g["_PERM_REPLIED"] = {}
        # Keep ok_p1-ok_p4 off the live opencode.db with a hermetic fake one, so
        # the case's outcome never depends on whatever this machine's
        # opencode.db happens to hold. Both captured sessions are registered in
        # the config AND are genuine roots there (parent_id NULL), which is what
        # AC4(d) now requires: a registered key qualifies because the DB PROVES
        # it is a root, not because no row came back. Pinning db_path at an
        # absent file instead would make con=None, and an unprovable lineage
        # must fail closed -- so the expected 2 replies would be wrong.
        _dbpath_ms = psweep_g["db_path"]
        # Pre-declared so this finally can still clean up if a later fixture
        # never gets built: without it, an exception between here and _tdir_ms's
        # assignment would make the finally itself raise NameError and mask the
        # real cause. The loop below skips the None entries.
        _tdir_p1 = _tdir_ms = None
        _tdir_p1, _tdb_p1 = _fake_session_db(
            [("ses_f91e010d7ffegdVQFmfTdOOBsU", None, _hs_dir),
             ("ses_b", None, _co_dir)])
        psweep_g["db_path"] = lambda: _tdb_p1
        # FIXTURE: the sweep replies only to asks whose sessionID resolves
        # through the DB lineage to a *monitored* root with auto_accept on, so
        # the two sessions the captured payloads came from must be registered
        # here -- as true roots in the fake DB above.
        cfg_on = dict(CONFIG_DEFAULTS, auto_accept_external_dirs=True,
                      project_dir=_hs_dir, monitored_sessions={
                          "ses_f91e010d7ffegdVQFmfTdOOBsU": dict(_perm_reg),
                          "ses_b": dict(_perm_reg)})
        t_p1 = desktop_permission_sweep(cfg_on)
        ok_p1 = t_p1 == 2 and sorted(perm_calls) == sorted([
            ("/permission/per_08242587e00102p72sl9a1pUUb/reply?directory=" + _hs_q,
             "POST", {"reply": "always"}),
            ("/api/session/ses_b/permission/per_v2_dir/reply", "POST", {"reply": "always"}),
        ])
        t_p2 = desktop_permission_sweep(cfg_on)  # dedupe: same ids within 15s
        ok_p2 = t_p2 == 0 and len(perm_calls) == 2
        cfg_off = dict(cfg_on, auto_accept_external_dirs=False)
        perm_calls.clear()
        t_p3 = desktop_permission_sweep(cfg_off)
        ok_p3 = t_p3 == 0 and perm_calls == []
        # a 403 HTML passthrough on every route must yield no replies, not a crash
        psweep_g["_PERM_REPLIED"] = {}
        psweep_g["_perm_request"] = lambda *a, **k: (403, _cf_403)
        ok_p4 = desktop_permission_sweep(cfg_on) == 0
        # a monitored session working in a directory that is neither a /project
        # worktree nor in the config: its asks must still be swept (regression:
        # the ask sat parked because the sweep never queried that directory)
        psweep_g["_PERM_REPLIED"] = {}
        _permreq_ms = psweep_g["_perm_request"]
        _retry_dir = r"C:\Users\dev\Desktop\demo_retry"
        _tdir_ms, _tdb_ms = _fake_session_db([("ses_retry", None, _retry_dir)])
        psweep_g["db_path"] = lambda: _tdb_ms
        _retry_q = urllib.parse.quote(_retry_dir, safe="")
        _catch_ms = []
        def _fake_ms(base, creds, path, method="GET", body=None, timeout=8):
            if path.endswith("/reply") or "/reply?" in path:
                _catch_ms.append(path)
                return (200, b"true")
            return {
                "/project": (200, b"[]"),
                "/permission?directory=" + _retry_q: (200, json.dumps([
                    {"id": "per_retry1", "sessionID": "ses_retry",
                     "permission": "external_directory",
                     "patterns": [r"C:\Users\dev\.cargo\registry\src\index.crates.io-1949cf8c6b5b557f\postcard-1.1.3\src\*"]}]).encode()),
            }.get(path, (403, _cf_403))
        psweep_g["_perm_request"] = _fake_ms
        # FIXTURE: enabled False + auto_accept True. The directory must now be
        # reached *without* the `enabled` gate (this is the regression), and the
        # ask must clear the per-session auto_accept gate -- ses_retry is a
        # registered key AND a true root in the fake DB, so the lineage walk
        # proves its own-root case and auto_accept is read from that entry.
        t_ms = desktop_permission_sweep(dict(
            cfg_on, monitored_sessions={"ses_retry": {"enabled": False,
                                                      "auto_accept": True}}))
        ok_p5 = t_ms == 1 and len(_catch_ms) == 1 \
            and _catch_ms[0] == "/permission/per_retry1/reply?directory=" + _retry_q
        # Read the permission event log BEFORE cleanup so the test can assert it
        _ev_log = _perm_tdir / "permission_events.jsonl"
        _evs = []
        try:
            _evs = [json.loads(l) for l in _ev_log.read_text("utf-8").splitlines()]
        except Exception:
            pass
    finally:
        psweep_g["desktop_sidecar"] = o_sidecar
        psweep_g["_perm_request"] = o_permreq
        psweep_g["_PERM_REPLIED"] = o_replied
        psweep_g["home_dir"] = o_home
        # db_path is restored HERE, paired with the rmtree below: on the happy
        # path it used to be restored mid-body, so an exception between the
        # fixture build and that line left db_path pointing into a temp dir that
        # this finally then never removed.
        psweep_g["db_path"] = _dbpath_ms
        for _d in (_tdir_p1, _tdir_ms, _perm_tdir):
            if _d:
                shutil.rmtree(_d, ignore_errors=True)
    ok_p6 = any(e.get("kind") == "permission_accept" and e.get("session") == "ses_b"
                and "Documents" in e.get("target", "") for e in _evs) \
        and any(e.get("session") == "ses_retry" for e in _evs)
    ok_perm = all([ok_p1, ok_p2, ok_p3, ok_p4, ok_p5, ok_p6])
    _perm_reason = ("Desktop sweep replies 'always' to v1+v2 external_directory asks "
                    "only, for monitored sessions with auto_accept (incl. subagent dirs)")
    results.append(("perm_auto_accept", "pass", "pass", ok_perm, _perm_reason, []))
    print("%-24s expect=%-8s got=%-8s %s  %s" % (
        "perm_auto_accept", "pass", "pass",
        "OK" if ok_perm else "MISMATCH", _perm_reason))

    # Perm auto-accept SCOPE. A Desktop ask is replied {"reply":"always"} only
    # when its sessionID resolves through the DB parent/child lineage to a
    # *monitored* root whose auto_accept is on -- `enabled` is hawk's steering
    # toggle and is never consulted -- AND the directory the ask lives in is
    # actually swept. The fixture is the captured live ask
    # per_0e239cd49001mHzNbP2OScZ3dC, raised by subagent
    # ses_f1dc638bcffeltLU7bPxDVWHQS under root ses_f5110ad49ffeoNa90jaza2FMdF:
    # that root IS a monitored key, but its entry carried auto_accept:false, and
    # the subagent's directory was never swept at all. Unprovable lineage (no
    # row, no DB, deeper than the bound, a cycle) must always fail CLOSED.
    # All offline; desktop_sidecar/_perm_request/db_path/connect_db/home_dir
    # faked, so nothing here touches the live DB, the sidecar or config.json.
    pms_g = desktop_permission_sweep.__globals__
    o_sidecar = pms_g["desktop_sidecar"]
    o_permreq = pms_g["_perm_request"]
    o_replied = pms_g["_PERM_REPLIED"]
    o_dbpath = pms_g["db_path"]
    o_connect = pms_g["connect_db"]
    o_home = pms_g["home_dir"]
    _pms_home = Path(tempfile.mkdtemp())
    pms_g["home_dir"] = lambda: _pms_home
    pms_g["desktop_sidecar"] = lambda: (
        "http://127.0.0.1:9999", {"username": "opencode", "password": "s3cret"})
    _pms_403 = b"<!doctype html>\n<html><head><title>Access denied | Cloudflare</title>"
    _pms_nodb = _pms_home / "no-such-opencode.db"   # db_path miss -> fail closed
    _pms_tmpdirs = []

    # live captured v1 record, verbatim
    LIVE_ASK = {
        "id": "per_0e239cd49001mHzNbP2OScZ3dC",
        "sessionID": "ses_f1dc638bcffeltLU7bPxDVWHQS",
        "permission": "external_directory",
        "patterns": [r"C:\Users\dev\coordinator\.opencode\tmp\*"],
        "metadata": {"filepath": r"C:\Users\dev\coordinator\.opencode\tmp\notif-row.png",
                     "parentDir": r"C:\Users\dev\coordinator\.opencode\tmp"},
        "always": [r"C:\Users\dev\coordinator\.opencode\tmp\*"],
        "tool": {"messageID": "msg_0e239c7580013MyxW58FcIw6LB",
                 "callID": "call_function_r8wn35nvu25b_1"},
    }
    LIVE_ROOT = "ses_f5110ad49ffeoNa90jaza2FMdF"
    LIVE_SUB = "ses_f1dc638bcffeltLU7bPxDVWHQS"
    LIVE_ROOTDIR = "C:/Users/dev/Documents/Default Project"   # DB spelling
    LIVE_SUBDIR = r"C:\Users\dev\coordinator\.opencode\tmp"  # the ask's target
    LIVE_Q = urllib.parse.quote(LIVE_ROOTDIR, safe="")
    _pms_reg = {"enabled": False, "auto_accept": True, "auto_continue": False,
                "project_dir": ""}          # `enabled` False is load-bearing
    _pms_base = dict(CONFIG_DEFAULTS, auto_accept_external_dirs=True)
    _pms_v1 = "/permission?directory=" + LIVE_Q
    _pms_v2 = "/api/permission/request?location%5Bdirectory%5D=" + LIVE_Q
    # Every sweep below puts the root's directory in the swept set through
    # cfg.project_dir, so each "must reply nothing" case really does FETCH the
    # ask before rejecting it -- otherwise those checks would pass vacuously
    # just because the directory was never queried.
    _pms_cfg = lambda ms, **kw: dict(_pms_base, project_dir=LIVE_ROOTDIR,
                                     monitored_sessions=ms, **kw)
    _pms_only_ask = {"/project": (200, b"[]"),
                     _pms_v1: (200, json.dumps([LIVE_ASK]).encode())}
    _pms_routes = dict(_pms_only_ask)   # no /project worktrees at all

    def _pms_db(rows):
        """_fake_session_db, plus this block's own temp-dir bookkeeping so the
        finally below still removes every fixture it made."""
        d, p = _fake_session_db(rows)
        _pms_tmpdirs.append(d)
        return d, p

    def _pms_connect_boom(p, attempts=5):
        raise RuntimeError("cannot open opencode DB %s: simulated" % p)

    def _pms_sweep(cfg, dbfile, routes, connect=None):
        """One sweep, hermetic. Records every sidecar call, GET and POST."""
        gets, posts = [], []

        def _fake(base, creds, path, method="GET", body=None, timeout=8):
            if method == "POST":
                posts.append((path, method, body))
                return (200, b"true")
            gets.append(path)
            return routes.get(path, (403, _pms_403))
        pms_g["_perm_request"] = _fake
        pms_g["db_path"] = lambda: dbfile
        pms_g["connect_db"] = connect or o_connect
        replied = {}
        pms_g["_PERM_REPLIED"] = replied
        return desktop_permission_sweep(cfg), gets, posts, replied

    def _pms_dirs(cfg, dbfile):
        """permission_directories() against a fake DB and a 403-everything
        sidecar, so the returned set is purely what discovery produced."""
        pms_g["_perm_request"] = lambda *a, **k: (403, _pms_403)
        pms_g["db_path"] = lambda: dbfile
        pms_g["connect_db"] = o_connect
        return permission_directories(cfg, "http://127.0.0.1:9999", {})

    _pms_live_db = _pms_db([(LIVE_ROOT, None, LIVE_ROOTDIR),
                            (LIVE_SUB, LIVE_ROOT, LIVE_ROOTDIR)])
    _live_path = _pms_live_db[1]
    _live_ms = {LIVE_ROOT: dict(_pms_reg)}
    _live_off = {LIVE_ROOT: {"enabled": True, "auto_accept": False,
                             "auto_continue": False, "project_dir": ""}}
    try:
        # 1: the live ask, root monitored with auto_accept on, enabled False ->
        #    accepted, and the exact v1 reply is recorded.
        t1, g1, p1_, r1 = _pms_sweep(_pms_cfg(_live_ms), _live_path, _pms_routes)
        ok_1 = t1 == 1 and p1_ == [("/permission/%s/reply?directory=%s"
                                    % (LIVE_ASK["id"], LIVE_Q), "POST",
                                    {"reply": "always"})]
        # 1b: THE D2 regression. Same qualifying root, but cfg.project_dir is now
        #     EMPTY and /project serves no worktree: the root's directory can only
        #     enter the swept set by discovery, so this is the "ask was invisible
        #     because its directory was never swept" case (and the offline proxy
        #     for the production visibility check).
        t1b, g1b, p1b, _ = _pms_sweep(
            dict(_pms_base, project_dir="", monitored_sessions=_live_ms),
            _live_path, _pms_routes)
        ok_1b = (_pms_v1 in g1b and t1b == 1
                 and p1b == [("/permission/%s/reply?directory=%s"
                              % (LIVE_ASK["id"], LIVE_Q), "POST",
                              {"reply": "always"})])

        # 2: same ask, root not registered -> nothing replied, no dedupe burnt.
        t2, g2, p2_, r2 = _pms_sweep(_pms_cfg({}), _live_path, _pms_routes)
        ok_2 = t2 == 0 and p2_ == [] and _pms_v1 in g2 and LIVE_ASK["id"] not in r2

        # 3: root registered but auto_accept False (enabled True on purpose:
        #    auto_accept decides, `enabled` is not consulted) -> nothing.
        t3, g3, p3_, r3 = _pms_sweep(_pms_cfg(_live_off), _live_path, _pms_routes)
        ok_3 = t3 == 0 and p3_ == [] and _pms_v1 in g3 and LIVE_ASK["id"] not in r3

        # 4a/b/c: subagent inheritance, all three polarities. The ask session
        #    itself is never a monitored key -- only its root is.
        _c_ok = "ses_child_of_monitored"
        _c_no = "ses_child_of_unregistered"
        _c_off = "ses_child_of_autohat"
        _sub_db = _pms_db([(LIVE_ROOT, None, LIVE_ROOTDIR),
                           (_c_ok, LIVE_ROOT, LIVE_ROOTDIR),
                           ("ses_unregistered_root", None, LIVE_ROOTDIR),
                           (_c_no, "ses_unregistered_root", LIVE_ROOTDIR),
                           ("ses_aa_false_root", None, LIVE_ROOTDIR),
                           (_c_off, "ses_aa_false_root", LIVE_ROOTDIR)])
        _sub_path = _sub_db[1]
        _r_ask = lambda sid, rid: [dict(LIVE_ASK, id=rid, sessionID=sid)]
        t4a, g4a, p4a, _ = _pms_sweep(
            _pms_cfg(_live_ms), _sub_path,
            {"/project": (200, b"[]"),
             "/permission?directory=" + LIVE_Q:
                 (200, json.dumps(_r_ask(_c_ok, "per_4a")).encode())})
        ok_4a = t4a == 1 and [x[0] for x in p4a] == ["/permission/per_4a/reply?directory=" + LIVE_Q]
        t4b, g4b, p4b, _ = _pms_sweep(
            _pms_cfg(_live_ms), _sub_path,
            {"/project": (200, b"[]"),
             "/permission?directory=" + LIVE_Q:
                 (200, json.dumps(_r_ask(_c_no, "per_4b")).encode())})
        ok_4b = t4b == 0 and p4b == [] and _pms_v1 in g4b
        t4c, g4c, p4c, _ = _pms_sweep(
            _pms_cfg(dict(_live_ms,
                          **{"ses_aa_false_root": _live_off[LIVE_ROOT]})), _sub_path,
            {"/project": (200, b"[]"),
             "/permission?directory=" + LIVE_Q:
                 (200, json.dumps(_r_ask(_c_off, "per_4c")).encode())})
        ok_4c = t4c == 0 and p4c == [] and _pms_v1 in g4c

        # 4d (AC4(d), re-pointed by reviewer): a registered key that IS a true
        #     root still qualifies -- but the DB has to PROVE it, with
        #     parent_id NULL, rather than the rule assuming it because no row
        #     came back. (A) genuine root: the ask's own sessionID is a
        #     registered key with parent_id NULL -> accepted.
        _root_db = _pms_db([(LIVE_SUB, None, LIVE_ROOTDIR),
                            (LIVE_ROOT, None, LIVE_ROOTDIR)])
        t4d, g4d, p4d, _ = _pms_sweep(_pms_cfg({LIVE_SUB: dict(_pms_reg)}),
                                     _root_db[1], _pms_routes)
        ok_4d = (t4d == 1 and p4d == [
            ("/permission/%s/reply?directory=%s" % (LIVE_ASK["id"], LIVE_Q),
             "POST", {"reply": "always"})])
        # 4e: the converse, and the reason 4d cannot be read as "a registered key
        #     is its own root": the SAME config key, with parent_id = LIVE_ROOT
        #     and auto_accept true of its own, is judged by the ROOT's entry --
        #     which is off here -- so nothing is replied.
        t4e, g4e, p4e, r4e = _pms_sweep(
            _pms_cfg({LIVE_SUB: dict(_pms_reg), LIVE_ROOT: _live_off[LIVE_ROOT]}),
            _pms_live_db[1], _pms_routes)
        ok_4e = (t4e == 0 and p4e == [] and _pms_v1 in g4e
                 and LIVE_ASK["id"] not in r4e)

        # 5: root resolution that cannot be proven must never auto-accept.
        t5a, g5a, p5a, r5a = _pms_sweep(
            _pms_cfg(_live_ms), _live_path,
            {"/project": (200, b"[]"),
             "/permission?directory=" + LIVE_Q:
                 (200, json.dumps(_r_ask("ses_not_in_the_db", "per_5a")).encode())})
        ok_5a = t5a == 0 and p5a == [] and _pms_v1 in g5a and "per_5a" not in r5a
        t5b, g5b, p5b, r5b = _pms_sweep(_pms_cfg(_live_ms), _pms_nodb, _pms_routes)
        ok_5b = t5b == 0 and p5b == [] and _pms_v1 in g5b and LIVE_ASK["id"] not in r5b
        t5c, g5c, p5c, r5c = _pms_sweep(_pms_cfg(_live_ms), _live_path,
                                         _pms_routes, connect=_pms_connect_boom)
        ok_5c = t5c == 0 and p5c == [] and _pms_v1 in g5c and LIVE_ASK["id"] not in r5c
        # 5d (AC5, re-pointed by reviewer): permission_directories' own DB block
        #     raising must not ABORT the sweep -- discovery still falls back to
        #     cfg.project_dir, so the sweep still issues the v1 GET and really
        #     does FIND the ask. What changes is the OUTCOME: with no provable
        #     lineage the ask is left hanging, 0 replies, no POST. The old form
        #     of this case asserted 1 reply off the removed "no DB, registered id
        #     is its own root" short circuit, which is the over-grant itself.
        t5d, g5d, p5d, r5d = _pms_sweep(_pms_cfg({LIVE_SUB: dict(_pms_reg)}),
                                      _live_path, _pms_routes, connect=_pms_connect_boom)
        ok_5d = (t5d == 0 and p5d == [] and _pms_v1 in g5d
                 and LIVE_ASK["id"] not in r5d)
        # 5e: THE LEAK this rule exists to close. The ask's sessionID is itself
        #     a registered key with auto_accept TRUE, and its root's entry is
        #     auto_accept FALSE -- the state of 27 of the 40 auto-accepting keys
        #     on this machine. The DB is present and readable, but connect_db
        #     raises at sweep time (the routine "database is locked" outcome on
        #     a 3.3 GB DB that opencode is writing to). Before the fix the
        #     con=None path judged the subagent by its OWN entry and answered
        #     with a durable {"reply":"always"}; it must now answer nothing,
        #     and must do so without raising. ok_5c is the same shape but with
        #     the subagent UNregistered, so it would not have caught this.
        t5e, g5e, p5e, r5e = _pms_sweep(
            _pms_cfg({LIVE_SUB: dict(_pms_reg), LIVE_ROOT: _live_off[LIVE_ROOT]}),
            _live_path, _pms_routes, connect=_pms_connect_boom)
        ok_5e = (t5e == 0 and p5e == [] and _pms_v1 in g5e
                 and LIVE_ASK["id"] not in r5e)

        # 6: the action filter still runs first, so a bash/edit ask is dropped
        #    before any lineage work. Counted differentially: a sweep carrying
        #    only a non-external_directory ask must open exactly as many DB
        #    connections as the identical sweep with an empty queue.
        _cnt = [0]

        def _c_count(p, attempts=5):
            _cnt[0] += 1
            return o_connect(p, attempts)
        _nofilter = {"/project": (200, b"[]"),
                     "/permission?directory=" + LIVE_Q: (200, b"[]")}
        t6e, _, _, _ = _pms_sweep(_pms_cfg(_live_ms), _live_path,
                                  _nofilter, connect=_c_count)
        _base_cnt = _cnt[0]
        _cnt[0] = 0
        _pms_nonauto = [dict(LIVE_ASK, id="per_6_bash", permission="bash",
                            patterns=["rm -rf *"]),
                        dict(LIVE_ASK, id="per_6_edit", action="edit", resources=["x"])]
        t6, g6, p6_, r6 = _pms_sweep(
            _pms_cfg(_live_ms), _live_path,
            {"/project": (200, b"[]"),
             "/permission?directory=" + LIVE_Q:
                 (200, json.dumps(_pms_nonauto).encode())}, connect=_c_count)
        ok_6 = (t6e == 0 and t6 == 0 and p6_ == [] and _pms_v1 in g6
                and "per_6_bash" not in r6 and "per_6_edit" not in r6
                and _cnt[0] == _base_cnt)
        # 6b: ... and the same two asks are still cleanly dropped (no exception
        #     escapes, nothing replied) when the DB cannot be opened at all.
        t6b, g6b, p6b, r6b = _pms_sweep(
            _pms_cfg(_live_ms), _pms_nodb,
            {"/project": (200, b"[]"),
             "/permission?directory=" + LIVE_Q:
                 (200, json.dumps(_pms_nonauto).encode())}, connect=_pms_connect_boom)
        ok_6b = t6b == 0 and p6b == [] and _pms_v1 in g6b and "per_6_bash" not in r6b

        # 7: the global kill switch is the outermost gate -- it precedes
        #    discovery, so not one sidecar call is made.
        t7, g7, p7_, r7 = _pms_sweep(
            dict(_pms_cfg(_live_ms), auto_accept_external_dirs=False),
            _live_path, _pms_routes)
        ok_7 = t7 == 0 and g7 == [] and p7_ == []

        # 8: a rejected ask must not burn its 15s dedupe slot, so registering
        #    the root and sweeping again IMMEDIATELY grants it (no sleep).
        _cfg8 = _pms_cfg({})
        t8a, g8a, p8a, r8 = _pms_sweep(_cfg8, _live_path, _pms_routes)
        _slot_free = LIVE_ASK["id"] not in r8
        pms_g["db_path"] = lambda: _live_path
        pms_g["connect_db"] = o_connect
        g8b, p8b = [], []

        def _fake8(base, creds, path, method="GET", body=None, timeout=8):
            (p8b if method == "POST" else g8b).append(path)
            return ((200, b"true") if method == "POST"
                    else _pms_routes.get(path, (403, _pms_403)))
        pms_g["_perm_request"] = _fake8
        t8b = desktop_permission_sweep(_pms_cfg(_live_ms))
        ok_8 = (t8a == 0 and p8a == [] and _pms_v1 in g8a and _slot_free
                and t8b == 1
                and p8b == ["/permission/%s/reply?directory=%s" % (LIVE_ASK["id"], LIVE_Q)])

        # 9: root-cause regression. A monitored root whose SUBAGENT works in a
        #    different directory that is not a /project worktree and not in the
        #    config: both spellings of that directory must be swept (DB stores
        #    '/', asks record '\') and the ask behind it replied.
        _sub_dir = "C:/Users/dev/Documents/Sub Tree"
        _sub_q = urllib.parse.quote(_sub_dir, safe="")
        _sub_np_q = urllib.parse.quote(os.path.normpath(_sub_dir), safe="")
        _reg_db = _pms_db([("ses_reg_root", None, LIVE_ROOTDIR),
                           ("ses_reg_sub", "ses_reg_root", _sub_dir)])
        t9, g9, p9, _ = _pms_sweep(
            _pms_cfg({"ses_reg_root": dict(_pms_reg)}),
            _reg_db[1],
            {"/project": (200, b"[]"),
             "/permission?directory=" + _sub_q: (200, json.dumps(
                 [dict(LIVE_ASK, id="per_9", sessionID="ses_reg_sub")]).encode()),
             "/permission?directory=" + _sub_np_q: (200, json.dumps(
                 [dict(LIVE_ASK, id="per_9", sessionID="ses_reg_sub")]).encode())})
        ok_9 = (t9 == 1
                and "/permission?directory=" + _sub_q in g9
                and "/permission?directory=" + _sub_np_q in g9
                and p9 == [("/permission/per_9/reply?directory=%s" % _sub_q,
                            "POST", {"reply": "always"})])

        # 10a: deeper than the lineage bound -> unprovable -> nothing replied.
        _deep = [("d1", None, LIVE_ROOTDIR)]
        for i in range(2, 6):
            _deep.append(("d%d" % i, "d%d" % (i - 1), LIVE_ROOTDIR))
        _deep_db = _pms_db(_deep)
        _deep_routes = lambda sid, rid: {"/project": (200, b"[]"),
                                         "/permission?directory=" + LIVE_Q:
                                             (200, json.dumps(_r_ask(sid, rid)).encode())}
        t10a, g10a, p10a, r10a = _pms_sweep(_pms_cfg({"d1": dict(_pms_reg)}),
                                            _deep_db[1], _deep_routes("d5", "per_10a"))
        ok_10a = t10a == 0 and p10a == [] and _pms_v1 in g10a and "per_10a" not in r10a
        # 10a2: the accept side of the same bound (3 parent hops resolves).
        t10a2, _, p10a2, _ = _pms_sweep(_pms_cfg({"d1": dict(_pms_reg)}),
                                         _deep_db[1], _deep_routes("d4", "per_10a2"))
        ok_10a2 = t10a2 == 1 and p10a2 == [
            ("/permission/per_10a2/reply?directory=%s" % LIVE_Q,
             "POST", {"reply": "always"})]
        # 10b: a parent_id cycle must fail closed AND the sweep must return.
        _cyc_db = _pms_db([("cyc_a", "cyc_b", LIVE_ROOTDIR),
                           ("cyc_b", "cyc_a", LIVE_ROOTDIR)])
        _t0 = time.time()
        t10b, g10b, p10b, r10b = _pms_sweep(_pms_cfg({"cyc_a": dict(_pms_reg)}),
                                            _cyc_db[1], _deep_routes("cyc_a", "per_10b"))
        _cyc_s = time.time() - _t0
        ok_10b = (t10b == 0 and p10b == [] and _pms_v1 in g10b
                  and "per_10b" not in r10b and _cyc_s < 10.0)
        # 10c: a root with an empty / NULL directory contributes no directory
        #      (and must not smuggle in os.path.normpath("") == ".").
        _nd_db = _pms_db([("nd_blank", None, ""), ("nd_null", None, None)])
        _nd_dirs = _pms_dirs(
            dict(_pms_base, project_dir="",
                 monitored_sessions={"nd_blank": dict(_pms_reg),
                                     "nd_null": dict(_pms_reg)}), _nd_db[1])
        ok_10c = ("." not in _nd_dirs and "" not in _nd_dirs
                  and "/" not in _nd_dirs and _nd_dirs == [])
        # 10d: the directory set is capped, and the root-first pass means every
        #      root's own directory survives while they fit inside the cap.
        _cap = pms_g.get("PERM_DIR_CAP")
        _many_db = _pms_db([("cap_r%03d" % i, None, "C:/syn/cap/r%03d" % i)
                            for i in range(200)])
        _cap_dirs = _pms_dirs(
            dict(_pms_base, project_dir="", monitored_sessions=dict(
                ("cap_r%03d" % i, dict(_pms_reg)) for i in range(200))), _many_db[1])
        ok_10d1 = isinstance(_cap, int) and 1 <= len(_cap_dirs) <= _cap
        # 200 roots x 2 spellings cannot all fit under a 128 cap, so the
        # root-first guarantee is pinned with a root count that DOES fit.
        _fits = max((_cap // 4) if isinstance(_cap, int) else 8, 1)
        _fits_db = _pms_db([("fit_r%02d" % i, None, "C:/syn/fit/r%02d" % i)
                            for i in range(_fits)])
        _fit_dirs = _pms_dirs(
            dict(_pms_base, project_dir="", monitored_sessions=dict(
                ("fit_r%02d" % i, dict(_pms_reg)) for i in range(_fits))), _fits_db[1])
        ok_10d2 = all(d in _fit_dirs for d in
                      ["C:/syn/fit/r%02d" % i for i in range(_fits)]
                      + [os.path.normpath("C:/syn/fit/r%02d" % i) for i in range(_fits)])

        # 11: `auto_accept` absent on a monitored entry means True (precedent:
        #     coordinator poll_once/auto_accept and dashboard's default entry).
        t11, _, p11, _ = _pms_sweep(_pms_cfg({LIVE_ROOT: {"enabled": False}}),
                                    _live_path, _pms_routes)
        ok_11 = t11 == 1 and p11 == [("/permission/%s/reply?directory=%s"
                                      % (LIVE_ASK["id"], LIVE_Q), "POST",
                                      {"reply": "always"})]

        # 12: the v2 shape (action + resources) is scoped the same way, and its
        #     reply is addressed to the ASKING session, not the root.
        _v2_id = "per_v2_live_dir"
        _v2_routes = {"/project": (200, b"[]"),
                      "/api/permission/request?location%5Bdirectory%5D=" + LIVE_Q:
                          (200, json.dumps({"location": {"directory": LIVE_ROOTDIR},
                                            "data": [{"id": _v2_id,
                                                      "sessionID": LIVE_SUB,
                                                      "action": "external_directory",
                                                      "resources": [LIVE_SUBDIR]}]}).encode())}
        t12, _, p12, _ = _pms_sweep(_pms_cfg(_live_ms), _live_path, _v2_routes)
        ok_12 = t12 == 1 and p12 == [("/api/session/%s/permission/%s/reply"
                                      % (LIVE_SUB, _v2_id), "POST",
                                      {"reply": "always"})]
        # 12b: ... and the same v2 ask under a non-qualifying root is dropped.
        t12b, g12b, p12b, r12b = _pms_sweep(_pms_cfg(_live_off), _live_path, _v2_routes)
        ok_12b = t12b == 0 and p12b == [] and _pms_v2 in g12b and _v2_id not in r12b
    finally:
        pms_g["desktop_sidecar"] = o_sidecar
        pms_g["_perm_request"] = o_permreq
        pms_g["_PERM_REPLIED"] = o_replied
        pms_g["db_path"] = o_dbpath
        pms_g["connect_db"] = o_connect
        pms_g["home_dir"] = o_home
        for _d in _pms_tmpdirs:
            shutil.rmtree(_d, ignore_errors=True)
        shutil.rmtree(_pms_home, ignore_errors=True)
    _pms_checks = [("ok_1", ok_1), ("ok_1b", ok_1b), ("ok_2", ok_2), ("ok_3", ok_3),
                   ("ok_4a", ok_4a), ("ok_4b", ok_4b), ("ok_4c", ok_4c), ("ok_4d", ok_4d),
                   ("ok_4e", ok_4e),
                   ("ok_5a", ok_5a), ("ok_5b", ok_5b), ("ok_5c", ok_5c), ("ok_5d", ok_5d),
                   ("ok_5e", ok_5e),
                   ("ok_6", ok_6), ("ok_6b", ok_6b), ("ok_7", ok_7), ("ok_8", ok_8),
                   ("ok_9", ok_9), ("ok_10a", ok_10a), ("ok_10a2", ok_10a2),
                   ("ok_10b", ok_10b), ("ok_10c", ok_10c), ("ok_10d1", ok_10d1),
                   ("ok_10d2", ok_10d2), ("ok_11", ok_11), ("ok_12", ok_12),
                   ("ok_12b", ok_12b)]
    _pms_bad = [nm for nm, v in _pms_checks if not v]
    ok_pms = not _pms_bad
    _pms_reason = ("auto-accept only for asks whose lineage root is a monitored "
                   "session with auto_accept; enabled never consulted; fail closed")
    results.append(("perm_monitored_scope", "pass", "pass", ok_pms,
                    _pms_reason if ok_pms else _pms_reason + " | failed: "
                    + ",".join(_pms_bad), []))
    print("%-24s expect=%-8s got=%-8s %s  %s" % (
        "perm_monitored_scope", "pass", "pass",
        "OK" if ok_pms else "MISMATCH",
        _pms_reason if ok_pms else "failed: " + ",".join(_pms_bad)))

    # Desktop question auto-answer. The fixture is the *captured* payload of the
    # M4-scope ask that blocked ses_f78822dbbffesmn3sw5u5J0e3W (probe API
    # session): a question-tool request in the v1 queue whose options carry a
    # "(Recommended)" marker. The sweep must pick the marker (even when not the
    # first option), fall back to the first option when no marker exists, leave
    # option-less (free-text) asks to a human, dedupe within the 15s window,
    # respect the config flag, tolerate the 403-HTML passthrough, and record
    # the answer to the event log. All offline; sidecar/_perm_request faked.
    qsweep_g = desktop_question_sweep.__globals__
    o_qsidecar = qsweep_g["desktop_sidecar"]
    o_qpermreq = qsweep_g["_perm_request"]
    o_qreplied = qsweep_g["_QUESTION_REPLIED"]
    o_qhome = qsweep_g["home_dir"]
    _qt_dir = Path(tempfile.mkdtemp())
    qsweep_g["home_dir"] = lambda: _qt_dir
    q_calls = []
    qsweep_g["desktop_sidecar"] = lambda: (
        "http://127.0.0.1:9999", {"username": "opencode", "password": "s3cret"})
    _hs_dir_q = r"C:\Users\dev\Desktop\demo_project"
    _hs_q3 = urllib.parse.quote(_hs_dir_q, safe="")
    _q_cf_403 = b"<!doctype html>\n<html><head><title>Access denied | Cloudflare</title>"
    def _fake_q(base, creds, path, method="GET", body=None, timeout=8):
        if path.endswith("/reply") or "/reply?" in path:
            q_calls.append((path, method, body))
            return (200, b"true")
        return {
            "/project": (200, json.dumps([
                {"id": "global", "worktree": "/"},
                {"id": "df32b552", "worktree": _hs_dir_q},
            ]).encode()),
            # v1: the M4-scope ask (captured), plus a no-marker + a free-text ask
            "/question?directory=" + _hs_q3: (200, json.dumps([
                {"id": "que_0977e687e001FF1iEbdXGXAWyw",
                 "sessionID": "ses_f78822dbbffesmn3sw5u5J0e3W",
                 "questions": [{
                     "question": "M3 is done and green. How should I proceed to M4?",
                     "header": "M4 scope",
                     "options": [
                         {"label": "M4 = sync engine (Recommended)",
                          "description": "core product"},
                         {"label": "M4 = stealth/finishing", "description": "M4 items"},
                         {"label": "I'll draft the full M4 plan", "description": "review"},
                         {"label": "Stop at M3", "description": "hold"},
                     ]},
                 ],
                 "tool": {"messageID": "msg_097796663001lSBivAGblCVdz3",
                          "callID": "YdeeFoNgFc4wrr5j2QbV9coazPZWFe4B"}},
                {"id": "que_no_marker", "sessionID": "ses_x",
                 "questions": [{"header": "which", "options": [
                     {"label": "A", "description": "d"}, {"label": "B", "description": "d"}]}]},
                {"id": "que_free_text", "sessionID": "ses_y",
                 "questions": [{"header": "describe", "options": []}]},
            ]).encode()),
            # v2: a (Recommended) marker on a non-first option
            "/api/question/request?location%5Bdirectory%5D=" + _hs_q3: (200, json.dumps({
                "location": {"directory": _hs_dir_q},
                "data": [{"id": "que_v2_1", "sessionID": "ses_z",
                          "questions": [{"header": "scope",
                                         "options": [{"label": "Basic", "description": ""},
                                                     {"label": "Full (Recommended)",
                                                      "description": ""}]}]}]}).encode()),
        }.get(path, (403, _q_cf_403))
    qsweep_g["_perm_request"] = _fake_q
    try:
        qsweep_g["_QUESTION_REPLIED"] = {}
        cfg_q = dict(CONFIG_DEFAULTS, auto_answer_questions=True,
                     project_dir=_hs_dir_q, monitored_sessions={})
        t_q1 = desktop_question_sweep(cfg_q)
        ok_q1 = t_q1 == 3 and sorted(q_calls) == sorted([
            ("/question/que_0977e687e001FF1iEbdXGXAWyw/reply?directory=" + _hs_q3,
             "POST", {"answers": [["M4 = sync engine (Recommended)"]]}),
            ("/question/que_no_marker/reply?directory=" + _hs_q3,
             "POST", {"answers": [["A"]]}),
            ("/api/session/ses_z/question/que_v2_1/reply", "POST",
             {"answers": [["Full (Recommended)"]]}),
        ])
        t_q2 = desktop_question_sweep(cfg_q)  # dedupe within the 15s window
        ok_q2 = t_q2 == 0 and len(q_calls) == 3
        cfg_qoff = dict(cfg_q, auto_answer_questions=False)
        q_calls.clear()
        ok_q3 = desktop_question_sweep(cfg_qoff) == 0 and q_calls == []
        qsweep_g["_QUESTION_REPLIED"] = {}
        qsweep_g["_perm_request"] = lambda *a, **k: (403, _q_cf_403)
        ok_q4 = desktop_question_sweep(cfg_q) == 0
        ok_q5 = _pick_answer({"options": [{"label": "X (Recommended)", "description": ""}]}) \
            == ["X (Recommended)"] \
            and _pick_answer({"options": [
                {"label": "A", "description": ""},
                {"label": "B (Recommended)", "description": ""}]}) == ["B (Recommended)"] \
            and _pick_answer({"options": []}) is None \
            and _pick_answer({}) is None
        _q_ev_log = _qt_dir / "permission_events.jsonl"
        _q_evs = []
        try:
            _q_evs = [json.loads(l) for l in _q_ev_log.read_text("utf-8").splitlines()]
        except Exception:
            pass
    finally:
        qsweep_g["desktop_sidecar"] = o_qsidecar
        qsweep_g["_perm_request"] = o_qpermreq
        qsweep_g["_QUESTION_REPLIED"] = o_qreplied
        qsweep_g["home_dir"] = o_qhome
        shutil.rmtree(_qt_dir, ignore_errors=True)
    ok_q6 = any(e.get("kind") == "question_answer"
                and e.get("session") == "ses_f78822dbbffesmn3sw5u5J0e3W"
                and "sync engine" in e.get("target", "") for e in _q_evs)
    ok_q = all([ok_q1, ok_q2, ok_q3, ok_q4, ok_q5, ok_q6])
    results.append(("question_auto_answer", "pass", "pass", ok_q,
                    "Desktop sweep auto-selects the (Recommended) option on question-tool asks (v1+v2; free-text skipped, dedupe + flag honored)", []))
    print("%-24s expect=%-8s got=%-8s %s  %s" % (
        "question_auto_answer", "pass", "pass",
        "OK" if ok_q else "MISMATCH",
        "Desktop sweep auto-selects the (Recommended) option on question-tool asks (v1+v2; free-text skipped, dedupe + flag honored)"))

    # build_plan_done_note must format all four fields (regression: 3 slots, 4
    # args raised "not all arguments converted during string formatting").
    note = build_plan_done_note({"title": "t"}, "plan complete", ["rule a"], "C:/p/done.md")
    ok_note = note.count("\nStopped at: ") == 1 and note.count("\nReason: plan complete") == 1 \
        and note.count("\nRules: rule a") == 1 and note.count("\nReport: C:/p/done.md") == 1 \
        and "%s" not in note
    results.append(("plan_done_note_format", "pass", "pass", ok_note,
                    "build_plan_done_note formats stamp, reason, rules, report", []))
    print("%-24s expect=%-8s got=%-8s %s  %s" % (
        "plan_done_note_format", "pass", "pass",
        "OK" if ok_note else "MISMATCH",
        "build_plan_done_note formats stamp, reason, rules, report"))

    # validate_config: valid config produces no warnings; missing project_dir
    # and broken ntfy produce expected messages.
    cfg_ok = dict(CONFIG_DEFAULTS)
    cfg_ok["project_dir"] = "/p"
    cfg_ok["notify"] = {"ntfy": {"server": "x", "topic": "t"}}
    ok_vc1 = validate_config(cfg_ok) == []
    cfg_empty = dict(CONFIG_DEFAULTS)
    ok_vc2 = any("project_dir" in p for p in validate_config(cfg_empty))
    cfg_ntfy = dict(CONFIG_DEFAULTS, project_dir="/p",
                    notify={"ntfy": {"server": "x", "topic": ""}})
    ok_vc3 = any("topic" in p for p in validate_config(cfg_ntfy))
    # ensure_ntfy_topic: an empty topic gets a unique random one, an existing
    # topic is kept as-is.
    cfg_t1, cfg_t2 = copy.deepcopy(cfg_ntfy), copy.deepcopy(cfg_ntfy)
    t1 = ensure_ntfy_topic(cfg_t1, persist=False)
    t2 = ensure_ntfy_topic(cfg_t2, persist=False)
    ok_vc4 = (bool(t1) and re.fullmatch(r"hawk-\d{12}", t1) is not None
              and t1 != t2 and cfg_t1["notify"]["ntfy"]["topic"] == t1)
    ok_vc5 = ensure_ntfy_topic(copy.deepcopy(cfg_ok), persist=False) is None
    ok_vc = all([ok_vc1, ok_vc2, ok_vc3, ok_vc4, ok_vc5])
    results.append(("validate_config", "pass", "pass", ok_vc,
                    "validate_config catches missing project_dir and broken notify channels", []))
    print("%-22s expect=%-8s got=%-8s %s  %s" % (
        "validate_config", "pass", "pass",
        "OK" if ok_vc else "MISMATCH",
        "validate_config catches missing project_dir and broken notify channels"))

    # --- Llama liveness watchdog tests ---
    # LW1: llama_server_alive returns True when llama_pid returns a PID.
    g_lw = evaluate.__globals__
    orig_lw_pid = g_lw["llama_pid"]
    orig_lw_alive = g_lw["llama_server_alive"]
    try:
        g_lw["llama_pid"] = lambda c: 12345
        real_alive_fn = g_lw.get("_REAL_LLAMA_SERVER_ALIVE", llama_server_alive)
        ok_lw1 = real_alive_fn(dict(CONFIG_DEFAULTS)) is True
        g_lw["llama_pid"] = lambda c: None
        ok_lw2 = real_alive_fn(dict(CONFIG_DEFAULTS)) is False
    finally:
        g_lw["llama_pid"] = orig_lw_pid
    ok_lw_alive = ok_lw1 and ok_lw2
    results.append(("llama_server_alive", "pass", "pass", ok_lw_alive,
                     "llama_server_alive: True with PID, False without", []))
    print("%-22s expect=%-9s got=%-9s %s  %s" % (
        "llama_server_alive", "pass", "pass",
        "OK" if ok_lw_alive else "MISMATCH",
        "llama_server_alive: True with PID, False without"))

    # LW2: evaluate() returns "llama_down" when the server is dead, even
    # during cooldown (the 2026-09-22 incident gap).
    parts, msgs = mkparts("Which do you want? If none, consider it closed.",
                          last_age_s=3600)
    d_lw = mk_repo()
    st_lw = {"last_evaluated_mid": "msg_test_final",
             "last_injected_at": int(time.time() * 1000) - 10 * 60 * 1000}
    orig_idle_lw = g_lw["llama_idle"]
    try:
        g_lw["llama_idle"] = (lambda c, s, w=None: True)
        g_lw["llama_server_alive"] = lambda c: False
        action_lw, reason_lw, hits_lw = evaluate(mkcfg(d_lw),
                                                  {"id": "ses_test", "title": "t"},
                                                  parts, msgs, dict(st_lw))
        ok_lw_dead = action_lw == "llama_down"
        # With server alive, the same state (in cooldown) returns "nothing".
        g_lw["llama_server_alive"] = lambda c: True
        action_lw2, reason_lw2, _ = evaluate(mkcfg(d_lw),
                                             {"id": "ses_test", "title": "t"},
                                             parts, msgs, dict(st_lw))
        ok_lw_alive_eval = action_lw2 == "nothing"
    finally:
        g_lw["llama_idle"] = orig_idle_lw
        g_lw["llama_server_alive"] = orig_lw_alive
        shutil.rmtree(d_lw, ignore_errors=True)
    ok_lw_eval = ok_lw_dead and ok_lw_alive_eval
    results.append(("llama_down_evaluate", "pass", "pass", ok_lw_eval,
                     "evaluate: llama_down bypasses cooldown; alive -> normal flow", []))
    print("%-22s expect=%-9s got=%-9s %s  %s" % (
        "llama_down_evaluate", "pass", "pass",
        "OK" if ok_lw_eval else "MISMATCH",
        "evaluate: llama_down bypasses cooldown; alive -> normal flow"))

    # LW3: apply_action "llama_down" handler: dry_run, success (7), failure (8).
    ga_lw = apply_action.__globals__
    o_save_lw = ga_lw["save_state"]
    o_respawn_lw = ga_lw["llama_respawn"]
    st_lw3 = {"continues": 0, "last_injected_at": 1234567890}
    ok_lw3 = False
    try:
        ga_lw["save_state"] = lambda st: None
        # dry_run: no side effects, returns 0
        r_dry = apply_action(dict(CONFIG_DEFAULTS), st_lw3,
                             {"id": "sx", "title": "t"}, "C:/p", [],
                             [{"id": "m1"}], "llama_down", "", [], dry_run=True)
        ok_dry = r_dry == 0 and "last_injected_at" in st_lw3
        # success: llama_respawn returns True -> return 7, clears cooldown
        st_lw3b = dict(st_lw3)
        ga_lw["llama_respawn"] = lambda c: True
        r_ok = apply_action(dict(CONFIG_DEFAULTS), st_lw3b,
                            {"id": "sx", "title": "t"}, "C:/p", [],
                            [{"id": "m1"}], "llama_down", "", [], dry_run=False)
        ok_respawn = (r_ok == 7 and "last_injected_at" not in st_lw3b
                      and "last_evaluated_mid" not in st_lw3b)
        # failure: llama_respawn returns False -> return 8, clears cooldown
        st_lw3c = dict(st_lw3)
        ga_lw["llama_respawn"] = lambda c: False
        r_fail = apply_action(dict(CONFIG_DEFAULTS), st_lw3c,
                              {"id": "sx", "title": "t"}, "C:/p", [],
                              [{"id": "m1"}], "llama_down", "", [], dry_run=False)
        ok_fail = (r_fail == 8 and "last_injected_at" not in st_lw3c
                   and "last_evaluated_mid" not in st_lw3c)
        ok_lw3 = ok_dry and ok_respawn and ok_fail
    finally:
        ga_lw["save_state"] = o_save_lw
        ga_lw["llama_respawn"] = o_respawn_lw
    results.append(("llama_down_apply", "pass", "pass", ok_lw3,
                     "apply_action llama_down: dry_run=0, success=7, fail=8, clears cooldown", []))
    print("%-22s expect=%-9s got=%-9s %s  %s" % (
        "llama_down_apply", "pass", "pass",
        "OK" if ok_lw3 else "MISMATCH",
        "apply_action llama_down: dry_run=0, success=7, fail=8, clears cooldown"))

    n_bad = sum(1 for r in results if not r[3])
    print()
    print("self-test: %d/%d scenarios passed" % (len(results) - n_bad, len(results)))
    return 0 if n_bad == 0 else 1


def poll_multisession(cfg, state, dry_run=False) -> int:
    """Poll all enabled roots; steer at most one (most-recently-active leaf) or
    none when any session is currently producing."""
    db = db_path()
    if not db.exists():
        log("error: opencode DB not found at %s" % db)
        return 1
    roots = monitored_roots(cfg)
    if not roots:
        return 0
    now_ms = int(time.time() * 1000)
    idle_ms = int(cfg.get("idle_minutes", DEFAULT_IDLE_MINUTES)) * 60_000
    acc = []
    primed = False
    with connect_db(db) as con:
        for sid in roots:
            try:
                base = session_row(con, sid)
            except Exception as e:
                log("multi: couldn't load root %s: %s" % (sid, e))
                continue
            if base is None:
                log("multi: root session %s not found; skipping" % sid)
                continue
            node = active_leaf(con, base)
            root_dir = (base.get("directory") or "").strip() or ""
            per = per_state(state, sid)
            parts = session_parts(con, node["id"])
            messages = session_messages(con, node["id"])
            last_mid = messages[-1]["id"] if messages else None
            last_t = last_part_time(parts)
            grew = bool(last_mid) and last_mid != per.get("last_evaluated_mid")
            busy_now = (now_ms - last_t) < idle_ms
            proj = (node.get("directory") or "").strip() or root_dir \
                or cfg.get("project_dir") or ""
            # primed marks a converged baseline: non-git project dirs leave
            # prev_head as "", so guarding on prev_head alone re-primes (and
            # skips evaluate) on every poll.
            if messages and not per.get("primed") and not per.get("prev_head") \
                    and not dry_run:
                per["prev_head"] = git_head(proj) or ""
                per["last_evaluated_mid"] = last_mid or ""
                per["primed"] = True
                save_state(state)
                primed = True
                grew = False
                log("primed baseline for %s: prev_head=%s" %
                    (sid, per["prev_head"]))
            acc.append({"sid": sid, "node": node, "per": per, "parts": parts,
                        "messages": messages,
                        "last_mid": last_mid, "last_t": last_t, "grew": grew,
                        "busy_now": busy_now, "root_dir": root_dir})
    if primed:
        return 0
    if any(x["grew"] and x["busy_now"] for x in acc):
        return 0
    # Stalled sessions are candidates too: a completed turn at rest past
    # idle, or an unfinished turn wedged past stalled_turn_minutes. Without
    # this, a stopped session produces no new message, grew never turns
    # true, and stall detection never runs for it. The re_stall cadence
    # inside evaluate() bounds re-injection.
    stall_ms = int(cfg.get("stalled_turn_minutes", 15)) * 60_000
    for x in acc:
        if x["grew"] or not x["parts"] or not x["last_t"]:
            continue
        silent_ms = now_ms - x["last_t"]
        if busy_from_messages(x["messages"]):
            x["grew"] = silent_ms > stall_ms
        else:
            x["grew"] = silent_ms > idle_ms
    candidates = [x for x in acc if x["grew"]]
    if not candidates:
        return 0
    pick = max(candidates, key=lambda x: x["node"]["time_updated"])
    project_dir = (pick["node"].get("directory") or "").strip() \
        or pick["root_dir"] or cfg.get("project_dir") or ""
    ms = (cfg.get("monitored_sessions") or {}).get(pick["sid"]) or {}
    node_cfg = dict(cfg)
    node_cfg["project_dir"] = project_dir
    action, reason, rule_hits = evaluate(node_cfg, pick["node"], pick["parts"],
                                         pick["messages"], pick["per"])
    return apply_action(cfg, state, pick["node"], project_dir, pick["parts"],
                        pick["messages"], action, reason, rule_hits,
                        dry_run=dry_run,
                        enable_continue=bool(ms.get("auto_continue", True)),
                        auto=bool(ms.get("auto_accept", True)),
                        node_sid=pick["sid"], node_per=pick["per"])


def poll_once(cfg, state, dry_run=False) -> int:
    db = db_path()
    if not db.exists():
        log("error: opencode DB not found at %s (set OPENCODE_DB)" % db)
        return 1

    with connect_db(db) as con:
        base = select_session(con, cfg)
        # Subagent (Task tool) work runs in child sessions. While a child has
        # a live turn, the root's transcript is stale, so stall rules would
        # misfire on the parent. Follow the active leaf for evaluate/continue.
        session = active_leaf(con, base)
        parts = session_parts(con, session["id"])
        messages = session_messages(con, session["id"])

    if not messages:
        log("no messages yet in session %s; nothing to do" % session["id"])
        if not dry_run and not state.get("prev_head"):
            state["prev_head"] = git_head(cfg["project_dir"]) or ""
            save_state(state)
        return 0

    # Priming pass: establish baseline (prev_head, last evaluated message) and bail.
    # primed marks a converged baseline: non-git project dirs leave prev_head
    # as "", so guarding on prev_head alone re-primes (and skips evaluate) on
    # every poll.
    if not state.get("primed") and not state.get("prev_head"):
        if dry_run:
            log("[dry-run] would print baseline, not saved")
            return 0
        state["prev_head"] = git_head(cfg["project_dir"]) or ""
        state["last_evaluated_mid"] = messages[-1]["id"]
        state["primed"] = True
        save_state(state)
        log("primed baseline: prev_head=%s last_msg=%s" %
            (state["prev_head"], state["last_evaluated_mid"]))
        return 0

    action, reason, rule_hits = evaluate(cfg, session, parts, messages, state)
    # The leaf may be a child session absent from monitored_sessions; inherit
    # the root's policy (auto_continue/auto_accept) for it.
    ms = ((cfg.get("monitored_sessions") or {}).get(session["id"])
          or (cfg.get("monitored_sessions") or {}).get(base["id"])
          or {})
    return apply_action(cfg, state, session, cfg["project_dir"], parts, messages,
                        action, reason, rule_hits, dry_run=dry_run,
                        enable_continue=bool(ms.get("auto_continue", True)),
                        auto=bool(ms.get("auto_accept", True)))


def _hawk_prompt(cfg, done=False) -> str:
    """Build the hawk nudge. Approval grant (if enabled) is the FIRST sentence:
    an unapproved plan makes the worker wait for user sign-off, so the grant
    must precede the generic re-check/verify instructions."""
    base = cfg["done_confirm_message"] if done else cfg["continue_message"]
    if not cfg.get("approve_pending_plans"):
        return base
    grant = ("Every pending plan and next step has been APPROVED by the "
             "user; no further approval is required. Proceed with "
             "implementation immediately instead of waiting for sign-off. ")
    return grant + base.replace("[coordinator] ", "", 1)


def apply_action(cfg, state, session, project_dir, parts, messages, action,
                 reason, rule_hits, dry_run=False, enable_continue=True, auto=True,
                 node_sid=None, node_per=None) -> int:
    target = node_per if node_per is not None else state
    if action in ("continue", "confirm_done"):
        if not enable_continue:
            if node_sid:
                log("continue suppressed for %s (auto_continue off)" % node_sid)
            # Advance last_evaluated_mid so this session stops being a
            # candidate; otherwise it's picked every poll and starves others.
            if messages:
                target["last_evaluated_mid"] = messages[-1]["id"]
                save_state(state)
            return 0
        last_mid = messages[-1]["id"]
        if action == "confirm_done":
            msg = _hawk_prompt(cfg, done=True)
            if dry_run:
                log("[dry-run] WOULD ASK CONFIRMATION OF STOP: DONE")
                return 0
        else:
            msg = _hawk_prompt(cfg, done=False)
            if dry_run:
                log("[dry-run] WOULD INJECT self-check prompt")
                return 0
        if cfg.get("restart_desktop_before_continue"):
            restart_desktop(cfg)
        if not send_continue(cfg, session["id"], project_dir, msg, auto=auto):
            # Delivery failed. Do NOT advance last_evaluated_mid/continues: the
            # session is still pending, so the next poll re-evaluates and retries
            # the delivery once the CLI is available again. Write an escalation
            # artifact so the failure is visible, but throttle it to the
            # re-stall cadence so a hard-down CLI does not spam one file per poll.
            now_ms = int(time.time() * 1000)
            last_fail = int(target.get("last_continue_fail_at") or 0)
            re_stall_ms = int(cfg.get("re_stall_minutes", 45)) * 60_000
            if now_ms - last_fail >= re_stall_ms:
                target["last_continue_fail_at"] = now_ms
                save_state(state)
                a = analyze(parts, messages)
                path = write_escalation(session, project_dir, reason or
                                        "self-check delivery failed (CLI unavailable); will retry",
                                        rule_hits, a, cfg)
                log("ESCALATE: self-check delivery failed; the staged injection at "
                    "last_evaluated_mid=%s is pending retry (check that OPENCODE_BIN "
                    "points at a working CLI) -> %s" % (last_mid, path))
                try:
                    import notify
                    notify.notify_escalation(cfg, session,
                                             reason or "self-check delivery failed (CLI unavailable); will retry",
                                             rule_hits, a, str(path))
                except Exception:
                    pass
            else:
                log("self-check delivery failed at last_evaluated_mid=%s; "
                    "retrying on a later poll (last artifact %ds ago)"
                    % (last_mid, (now_ms - last_fail) // 1000))
            return 2
        target["last_evaluated_mid"] = last_mid
        target["prev_head"] = git_head(project_dir) or target.get("prev_head")
        if action == "continue":
            target["continues"] = target.get("continues", 0) + 1
        target.pop("last_continue_fail_at", None)
        save_state(state)
        if action == "continue":
            try:
                import notify
                notify.record_event(
                    cfg, "continue", "Self-check injected",
                    "[hawk] <b>self-check injected</b>\n<i>%s</i>\n\nSession: %s"
                    % (reason, (session.get("title") or "")[:60]),
                    eid="cont:%s:%s" % (session["id"], last_mid),
                    meta={"session": str(session["id"])[-8:],
                          "title": (session.get("title") or "")[:60]})
            except Exception:
                pass
        if action == "confirm_done":
            log("CONFIRMATION ASKED (first STOP: DONE)")
            try:
                import notify
                a = analyze(parts, messages)
                notify.notify_escalation(cfg, session, reason, rule_hits, a, "",
                                         kind="confirm_done")
            except Exception:
                pass
        else:
            log("SELF-CHECK injected")
        injected_id = await_injected_message_id(
            session["id"], max((m["time"] for m in messages if m.get("role") == "user"), default=0))
        if injected_id:
            target["last_injected_mid"] = injected_id
        target["last_injected_at"] = int(time.time() * 1000)
        save_state(state)
        return 3

    if action == "llama_down":
        # Liveness watchdog: llama-server process is gone. Respawn it via the
        # bat file and notify the user. This is NOT a session-level event —
        # it's infrastructure. We do NOT advance last_evaluated_mid so the
        # session is re-evaluated on the next poll (by which time the server
        # should be back).
        if dry_run:
            log("[dry-run] WOULD RESPAWN llama-server (process not found)")
            return 0
        target.pop("last_injected_at", None)  # clear cooldown so next poll proceeds
        save_state(state)
        # Lock-guarded so a concurrent monitor supervisor respawn never
        # double-spawns; when the supervisor already holds the lock, treat
        # the respawn as handled by it.
        ok = False
        try:
            import llama_restart
            if llama_restart._lock("flow:llama_down"):
                try:
                    ok = llama_respawn(cfg)
                finally:
                    llama_restart._unlock()
            else:
                ok = True  # supervisor is already on it
        except Exception:
            ok = llama_respawn(cfg)
        try:
            import notify
            if ok:
                notify.send_text(
                    cfg, "[hawk] <b>llama-server auto-restarted</b>\n"
                         "<i>process was dead; watchdog respawned it via bat file</i>",
                    kind="llama",
                    eid="llamarestart:%d" % int(time.time()),
                    meta={"kind": "llama_down"})
            else:
                notify.send_text(
                    cfg, "[hawk] <b>llama-server DOWN</b>\n"
                         "<i>watchdog respawn FAILED — manual restart needed</i>",
                    kind="llama",
                    eid="llamafail:%d" % int(time.time()),
                    meta={"kind": "llama_down"})
        except Exception:
            pass
        return 7 if ok else 8

    if action == "done":
        # Build plan finished — a POSITIVE stop, not an error. No further
        # self-checks; write a "complete" artifact and reset the counter.
        last_mid = messages[-1]["id"]
        if dry_run:
            log("[dry-run] WOULD MARK BUILD PLAN COMPLETE: %s" % reason)
            return 0
        target["last_evaluated_mid"] = last_mid
        target["continues"] = 0
        target["done_mid"] = last_mid
        save_state(state)
        a = analyze(parts, messages)
        path = write_plan_done(session, project_dir, reason, rule_hits, a, cfg)
        log("DONE: build plan complete -> %s" % path)
        if cfg.get("record_escalations_to_session", True):
            db = db_path()
            note = build_plan_done_note(session, reason, rule_hits, str(path))
            if append_session_note(db, session["id"], note, _model_field(session)):
                log("DONE: recorded in session transcript")
        try:
            import notify
            notify.send_text(
                cfg, "[hawk] <b>build plan complete</b>\n<i>%s</i>\n\nSession: %s"
                % (reason, (session.get("title") or "")[:60]),
                eid="plandone:%s:%s" % (session["id"], last_mid),
                meta={"session": str(session["id"])[-8:],
                      "title": (session.get("title") or "")[:60]})
        except Exception as _ne:
            log("DONE: notification failed: %s" % _ne)
        return 6

    # action == nothing
    log("no action: %s" % reason)
    return 0


def _poll_worker(cfg, state, args):
    """Run one sweep+eval cycle inside a daemon thread so a single wedged
    sub-call (GPU/PDH, subprocess, network) can never freeze the monitor loop;
    the watchdog in main() bounds the join and abandons a wedged worker."""
    try:
        if not args.dry_run and cfg.get("auto_accept_external_dirs", True):
            desktop_permission_sweep(cfg)  # catch asks missed by SSE
        if not args.dry_run and cfg.get("auto_answer_questions", True):
            desktop_question_sweep(cfg)  # question-tool asks block the worker turn; catch misses
        if monitored_roots(cfg):
            poll_multisession(cfg, state, dry_run=args.dry_run)
        elif _single_session_enabled(cfg, cli_override=bool(args.session_id)):
            poll_once(cfg, state, dry_run=args.dry_run)
        else:
            log("no enabled sessions; nothing to auto-continue")
    except Exception as e:
        log("poll error: %s" % e)


def main():
    ap = argparse.ArgumentParser(description="self-check local-agent coordinator")
    ap.add_argument("--once", action="store_true", help="one poll pass (default)")
    ap.add_argument("--monitor", action="store_true", help="poll loop")
    ap.add_argument("--interval", type=int, default=None, help="monitor interval minutes")
    ap.add_argument("--session-id", default=None, help="override watched session id")
    ap.add_argument("--dry-run", action="store_true",
                    help="evaluate and print the verdict without sending or saving state")
    ap.add_argument("--self-test", action="store_true",
                    help="run synthetic rule-engine scenarios and exit")
    ap.add_argument("--install-task", action="store_true",
                    help="print the schtasks command to schedule every N minutes")
    args = ap.parse_args()

    if args.self_test:
        sys.exit(self_test())

    cfg = load_config()
    new_topic = ensure_ntfy_topic(cfg)
    if new_topic:
        log("ntfy: generated topic %s for this install - subscribe to it in the "
            "ntfy app to get phone alerts" % new_topic)
    for prob in validate_config(cfg):
        log("WARNING: %s" % prob)
    if args.session_id:
        cfg["session_id"] = args.session_id
    state = load_state()

    if args.install_task:
        py = shutil.which("python") or sys.executable
        script = Path(__file__).resolve()
        every = cfg.get("poll_minutes", DEFAULT_POLL_MINUTES)
        print('schtasks /Create /F /TN "HawkCoordinator" /SC MINUTE /MO %d '
              '/TR "\\"%s\\" \\"%s\\" --once" '
              '/RL LIMITED' % (every, py, script))
        return 0

    if args.monitor:
        interval = args.interval or int(cfg.get("poll_minutes", DEFAULT_POLL_MINUTES))
        log("monitor loop every %d min (dry_run=%s)" % (interval, args.dry_run))
        stop_perm = threading.Event()
        if not args.dry_run and cfg.get("auto_accept_external_dirs", True):
            threading.Thread(target=permission_event_watcher, args=(stop_perm,),
                             daemon=True, name="perm-watcher").start()
        last_poll = 0.0
        last_llama_check = 0.0
        paused_logged = False
        try:
            while True:
                try:
                    cfg = load_config()  # pick up dashboard setting changes each tick
                    if args.interval is None:
                        interval = int(cfg.get("poll_minutes", DEFAULT_POLL_MINUTES))
                    if apply_control(cfg, state):
                        last_poll = 0.0  # poll_now: run on next tick
                except Exception as e:
                    log("control error: %s" % e)
                # Llama liveness supervisor - independent of session polling.
                # A dead server is infrastructure, not a session event, so it
                # must be handled even while the monitor is paused (the
                # session-flow watchdogs early-return on busy sessions and
                # never reach their liveness check - the 2026-09-25
                # 16:21 -> 17:14 dead-server gap).
                tick_now = time.time()
                if tick_now - last_llama_check >= 30.0:
                    last_llama_check = tick_now
                    try:
                        import llama_restart
                        llama_restart.supervise(load_config())
                    except Exception as e:
                        log("llama supervise error: %s" % e)
                if state.get("paused"):
                    if not paused_logged:
                        log("paused by dashboard control; polling suspended")
                        paused_logged = True
                    time.sleep(1)
                    continue
                paused_logged = False
                if time.time() - last_poll >= interval * 60:
                    budget = interval * 60 + int(
                        cfg.get("cycle_watchdog_grace_s", 1500))
                    worker = threading.Thread(
                        target=_poll_worker, args=(cfg, state, args),
                        daemon=True, name="poll-worker")
                    worker.start()
                    worker.join(timeout=budget)
                    if worker.is_alive():
                        log("poll cycle watchdog fired after %ds: abandoning "
                            "wedged worker thread" % budget)
                    last_poll = time.time()
                    state["last_poll_at"] = int(last_poll * 1000)
                    try:
                        save_state(state)
                    except Exception as e:
                        # a transient state write must not kill the poll loop;
                        # the next tick will retry with the same in-memory state
                        log("state save error: %s" % e)
                try:
                    import notify
                    notify.drain_replies(
                        lambda sid, choice: _reply_to_session(cfg, sid, choice))
                    notify.poll_ntfy_replies(
                        lambda sid, text: _reply_to_session(cfg, sid, text))
                except Exception as e:
                    log("notify poll error: %s" % e)
                time.sleep(1)
        except KeyboardInterrupt:
            stop_perm.set()
            log("monitor stopped")
            return 0

    if not args.dry_run and cfg.get("auto_accept_external_dirs", True):
        try:
            desktop_permission_sweep(cfg)
        except Exception as e:
            log_debug("permission sweep error: %s" % e)
    if monitored_roots(cfg):
        return poll_multisession(cfg, state, dry_run=args.dry_run)
    if _single_session_enabled(cfg, cli_override=bool(args.session_id)):
        return poll_once(cfg, state, dry_run=args.dry_run)
    log("no enabled sessions; nothing to auto-continue")
    return 0


if __name__ == "__main__":
    code = main()
    sys.exit(code)