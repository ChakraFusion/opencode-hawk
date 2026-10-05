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
import html
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

from . import hawk_linux

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
    # The injected prompts are deliberately generic: no plan file names and no claims about approvals, so they fit
    # any workflow. Workflow-specific wording (plan locations, approval rules) belongs in config.json.
    "continue_message": (
        "You stopped. Check your plan and the work done so far. If any step is "
        "still open, continue with the next one and work until it is complete "
        "and verified, then go on with the following steps instead of stopping "
        "early. Only pause for a real blocker or a decision a human must make: "
        "reply STOP: BLOCKED <reason> or STOP: NEEDS_DECISION <question>. "
        "Reply STOP: DONE only when every step of the plan is complete."
    ),
    "approve_pending_plans": False,  # opt-in: prefix the prompts with an explicit "plans approved" grant
    "done_confirm_message": (
        "You replied STOP: DONE. Before confirming, go through your plan item by "
        "item and list each one with its evidence (test run, commit, file, "
        "screenshot). Anything not complete and verified is OPEN, including items "
        "you would call minor, documentation-only, deferred, a known gap or an "
        "accepted limitation. If any item is open, do not confirm: continue "
        "working on it now. If only a human can settle it, reply "
        "STOP: NEEDS_DECISION <question>. Reply STOP: DONE again only if the "
        "list has no open item."
    ),
    # A STOP: DONE while the session's task list has open tasks, or whose own text still names open work ("7/9 screens",
    # "remaining items", "deferred", ...) is sent back with those lines quoted,
    # at most done_claim_max_rejects times per run; after that the normal
    # double-tap applies and the done alert flags the open items.
    "done_claim_check": True,
    "done_claim_max_rejects": 2,
    "done_open_items_message": (
        "You replied STOP: DONE, but work is still open (your task list and your own message):\n"
        "{items}\n"
        "A plan is done only when nothing is open. Finish these items and verify "
        "them now; do not reclassify them as minor, documentation-only or "
        "non-blocking. If only a human can settle one, reply "
        "STOP: NEEDS_DECISION <question>."
    ),
    "confirm_install_cli": False,
    "continue_via_attach": True,  # prefer Desktop sidecar so the GUI streams live
    "llama_pid": 0,               # 0 = auto-detect the llama-server process by name
    "llama_api_port": 1234,       # where llama-server serves its OpenAI API (/slots, /metrics)
    "llama_bat_path": "",         # script that starts llama-server; used to start it again when it is down
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
    "gpu_vram_total_mb": 0,       # GPU VRAM total (MB) that pins the VRAM chart Y-axis + "used/total" %. 0 = auto-detect
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
        # or a third-party harness such as tests/selftest_notify.py, which is safe only
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
_USER_HOME: Path | None = None


def home_dir() -> Path:
    """Folder for config.json, state and logs.

    $COORD_HOME when set; else the repository root when running from a source
    checkout (pyproject.toml next to the package); else a per-user folder:
    %APPDATA%\\opencode-hawk on Windows, $XDG_CONFIG_HOME/opencode-hawk
    (~/.config/opencode-hawk) elsewhere."""
    env = os.environ.get("COORD_HOME")
    if env:
        return Path(env)
    root = Path(__file__).resolve().parent.parent
    if (root / "pyproject.toml").exists():
        return root
    global _USER_HOME
    if _USER_HOME is None:
        if os.name == "nt":
            base = Path(os.environ.get("APPDATA") or Path.home() / "AppData" / "Roaming")
        else:
            base = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
        _USER_HOME = base / "opencode-hawk"
        _USER_HOME.mkdir(parents=True, exist_ok=True)
    return _USER_HOME


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
    n = cfg.get("notify") or {}
    ntfy = n.get("ntfy") or {}
    if ntfy and not str(ntfy.get("topic") or "").strip():
        problems.append("config: notify.ntfy is present but topic is missing - "
                        "ntfy alerts will not send")
    if not str(cfg.get("llama_bat_path") or "").strip():
        problems.append("config: llama_bat_path is empty - a llama-server that dies cannot be "
                        "started again (set it to the script that starts your server)")
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


def open_todos(session_id) -> list:
    """The session's open tasks from OpenCode's own task list (todowrite, the
    list the GUI shows): pending or in_progress, as "[status] content". Empty
    when the list or the DB is unavailable (older OpenCode, tests)."""
    try:
        con = connect_db(db_path(), attempts=1)
        try:
            rows = con.execute(
                "SELECT content, status FROM todo WHERE session_id=? AND status IN ('pending','in_progress') "
                "ORDER BY position", (session_id,)).fetchall()
        finally:
            con.close()
        return ["[%s] %s" % (st, (c or "")[:200]) for c, st in rows]
    except Exception:
        return []


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
                # A compaction summary often quotes "STOP: DONE" from the rules; it is not a done claim.
                if role == "assistant" and (mm.get("summary") or (mm.get("agent") or mm.get("mode")) == "compaction"):
                    role = "summary"
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
        if p.get("role") in ("user", "summary"):
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


# ── Continue injection ───────────────────────────────────────────────────────
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
    if os.name != "nt":
        found = hawk_linux.desktop_sidecar()
        return found[0] if found else None
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
    lines.append("- Read the transcript; if the work is genuinely complete, reply `continue` "
                 "manually (or let the worker end with STOP: DONE).")
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


def session_agent(session_id) -> str:
    """The agent the session works with: the newest assistant message's agent
    (compaction excluded). Injections and notes must carry it: without one,
    OpenCode switches the session to its default agent (or "build")."""
    try:
        con = connect_db(db_path(), attempts=1)
        try:
            for (d,) in con.execute(
                    "SELECT data FROM message WHERE session_id=? ORDER BY time_created DESC LIMIT 40", (session_id,)):
                md = parse_part_data(d)
                if isinstance(md, dict) and md.get("role") == "assistant" and not md.get("summary"):
                    ag = md.get("agent") or md.get("mode")
                    if ag and ag != "compaction":
                        return ag
        finally:
            con.close()
    except Exception:
        pass
    return ""


def append_session_note(db: Path, session_id: str, text: str, model: dict) -> bool:
    """Insert a synthetic user message + text part into the opencode DB so the
    escalation is visible in the session transcript (the same shape opencode
    uses for its own injected prompts). Returns False and never raises on
    failure — an unrecorded escalation is not fatal."""
    now = int(time.time() * 1000)
    mid = _gen_opencode_id("msg_")
    pid = _gen_opencode_id("prt_")
    msg_data = {"role": "user", "time": {"created": now}, "agent": session_agent(session_id) or "build", "model": model}
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
        open_items = []
        if cfg.get("done_claim_check", True):
            # First the task list (the agent's own plan), then its own words.
            open_items = (open_todos(session.get("id")) + done_open_items(a["last_assistant"]))[:8]
        rejects = int(state.get("done_rejects") or 0)
        if open_items and state.get("rejected_done_mid") != done_mid \
                and rejects < int(cfg.get("done_claim_max_rejects", 2)):
            # The claim contradicts itself: send the open lines back.
            state["rejected_done_mid"] = done_mid
            state["done_rejects"] = rejects + 1
            state.pop("pending_done_mid", None)
            return "done_rejected", ("STOP: DONE names %d open item(s); sending them back (%d/%s)"
                                     % (len(open_items), rejects + 1, cfg.get("done_claim_max_rejects", 2))), open_items
        if state.get("rejected_done_mid") == done_mid:
            # Already sent back; wait for the worker's answer (cadence below).
            pending_done = ""
        elif pending_done and pending_done != done_mid:
            # Second, distinct STOP: DONE -> worker genuinely finished.
            state.pop("pending_done_mid", None)
            return "done", ("worker confirmed build plan complete (%s)" % a["plan_done_marker"]), [a["plan_done_marker"]]
        elif not pending_done:
            # First STOP: DONE -> one-time confirmation, no continue text.
            state["pending_done_mid"] = done_mid
            return "confirm_done", ("worker says build plan complete (%s); asking once to confirm" % a["plan_done_marker"]), [a["plan_done_marker"]]
        # The same STOP: DONE message is already pending and was already asked
        # about. Fall through to the cadence logic below - do not re-ask on the
        # same message.
    elif pending_done:
        # Worker sent something new that is NOT STOP: DONE -> back to work.
        state.pop("pending_done_mid", None)
    if not a["plan_done"] and a["has_text"] and state.get("done_rejects"):
        # The worker went back to work: a later done claim gets the check again.
        state.pop("done_rejects", None)
        state.pop("rejected_done_mid", None)

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


# ── Done-claim check ─────────────────────────────────────────────────────────
# Lines of a STOP: DONE message that still name open work. Deliberately simple
# and explainable: a count below its total ("7/9 screens") or a word that
# admits unfinished work. A line that negates it ("no open items",
# "remaining: none") does not count.
_OPEN_WORDS = re.compile(
    r"\b(remaining|remains|still open|open items?|not yet|todo|to-do|deferred|"
    r"skipped|gaps?|missing|not captured|not committed|not implemented|"
    r"not verified|unverified|absent|incomplete|partially|pending|"
    r"(?:accepted|known) limitations?|follow-ups?|left to do|"
    r"blocked|environment[- ]blocked|could not|couldn't|cannot|can't|unable|not tested|untested|prevents?|"
    r"previous(?:ly)? (?:verif\w*|confirm\w*))\b|\u26a0", re.I)
_NEGATED = re.compile(
    r"\b(no|zero|0|none|nothing|without|not)\b[^.;\n]{0,25}\b(remaining|open|todo|gaps?|missing|"
    r"pending|deferred|skipped|follow-ups?|blocked)\b|"
    r"\b(remaining|open items?|todo|gaps?|missing|pending|deferred)\b\s*[:=-]?\s*"
    r"(none|0|nothing|n/a)\b", re.I)
_FRACTION = re.compile(r"(?<![\w/.])(\d{1,4})\s*/\s*(\d{1,4})(?![\w/.])")


def done_open_items(text: str, limit: int = 6) -> list:
    """The lines of a done message that still name open work (max `limit`)."""
    found = []
    for raw in (text or "").splitlines():
        line = raw.strip().strip("-*\u2022 ").strip()
        if not line or any(m in line for m in PLAN_DONE_MARKERS):
            continue
        short = any(int(a) < int(b) for a, b in _FRACTION.findall(line))
        word = bool(_OPEN_WORDS.search(line)) and not _NEGATED.search(line)
        if short or word:
            found.append(line.replace("**", "")[:200])
            if len(found) >= limit:
                break
    return found


# ── Poll pass ────────────────────────────────────────────────────────────────
def self_test():
    """Run the scenarios in tests/selftest_coordinator.py (source checkout only)."""
    tests = Path(__file__).resolve().parent.parent / "tests"
    if not (tests / "selftest_coordinator.py").exists():
        print("self-test: tests/ not found (they ship with the source checkout, "
              "not the installed package)")
        return 1
    sys.path.insert(0, str(tests))
    import selftest_coordinator
    return selftest_coordinator.self_test()


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
    if action in ("continue", "confirm_done", "done_rejected"):
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
        elif action == "done_rejected":
            msg = cfg["done_open_items_message"].replace(
                "{items}", "\n".join("- " + i for i in rule_hits))
            if dry_run:
                log("[dry-run] WOULD SEND BACK STOP: DONE (open items)")
                return 0
        else:
            msg = _hawk_prompt(cfg, done=False)
            if dry_run:
                log("[dry-run] WOULD INJECT self-check prompt")
                return 0
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
                    from . import notify
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
                from . import notify
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
                from . import notify
                a = analyze(parts, messages)
                notify.notify_escalation(cfg, session, reason, rule_hits, a, "",
                                         kind="confirm_done")
            except Exception:
                pass
        elif action == "done_rejected":
            log("DONE REJECTED (open items): %s" % " | ".join(rule_hits))
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
            from . import llama_restart
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
            from . import notify
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
        open_items = (open_todos(session.get("id")) + done_open_items(a["last_assistant"]))[:8]
        if open_items:
            reason += "; WARNING: the agent's message still names open items"
            rule_hits = list(rule_hits) + ["open item: " + i for i in open_items]
        path = write_plan_done(session, project_dir, reason, rule_hits, a, cfg)
        log("DONE: build plan complete -> %s" % path)
        if cfg.get("record_escalations_to_session", True):
            db = db_path()
            note = build_plan_done_note(session, reason, rule_hits, str(path))
            if append_session_note(db, session["id"], note, _model_field(session)):
                log("DONE: recorded in session transcript")
        try:
            from . import notify
            warn = ("\n\n\u26a0\ufe0f <b>The agent still mentions open items:</b>\n%s\nReply to send it back to work."
                    % "\n".join("- " + html.escape(i, quote=False) for i in open_items)) if open_items else ""
            notify.send_text(
                cfg, "[hawk] <b>build plan complete</b>\n<i>%s</i>\n\nSession: %s%s"
                % (html.escape(reason, quote=False), (session.get("title") or "")[:60], warn),
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


def main(argv=None):
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
                    help="print the scheduler entry (schtasks / cron) to run every N minutes")
    args = ap.parse_args(argv)

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
        py = sys.executable
        every = cfg.get("poll_minutes", DEFAULT_POLL_MINUTES)
        if os.name != "nt":
            print("# add to `crontab -e`:")
            print("*/%d * * * * %s -m opencode_hawk poll" % (every, py))
            return 0
        print('schtasks /Create /F /TN "HawkCoordinator" /SC MINUTE /MO %d '
              '/TR "\\"%s\\" -m opencode_hawk poll" '
              '/RL LIMITED' % (every, py))
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
                        from . import llama_restart
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
                    from . import notify
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


# Moved to probes.py; bound here so `coordinator.<name>` keeps working (one shared namespace).
from .probes import (  # noqa: E402
    ProcessMemIo,
    ProcessTimes,
    _DISK_IDLE_COUNTER,
    _GPU_MEM_COUNTER,
    _GPU_SAMPLER_LOCK,
    _GPU_SAMPLER_STARTED,
    _GPU_SYS_COUNTER,
    _GVRAM,
    _PDH_GPU,
    _PDH_GPU_LOCK,
    _PDH_SAMPLE,
    _PROC_MEMIO,
    _PROC_TIMES,
    _SRAM,
    _arr_max,
    _arr_vals,
    _dxgi_dedicated_vram_mb,
    _ensure_gpu_sampler,
    _gpu_group,
    _gpu_sampler_loop,
    _pdh_gpu_init,
    _pdh_gpu_read,
    _reply_to_session,
    _session_project_dir,
    build_continue_cmd,
    gpu_adapter_totals,
    gpu_adapters_engine_pct,
    gpu_adapters_usage_mb,
    gpu_engine_max,
    gpu_vram_total_mb,
    gpu_vram_used_mb,
    llama_api_base,
    llama_cpu_frac,
    llama_gpu_util,
    llama_idle,
    llama_idle_sustained,
    llama_mem_io,
    llama_pid,
    llama_respawn,
    llama_server_alive,
    llama_slots,
    llama_slots_processing,
    llama_token_speeds,
    physical_disk_active_pct,
    send_continue,
    system_ram_total_mb,
    system_ram_used_mb,
)


# Moved to desktop.py; bound here so `coordinator.<name>` keeps working (one shared namespace).
from .desktop import (  # noqa: E402
    PERM_AUTO_ACTIONS,
    PERM_DIR_CAP,
    PERM_LINEAGE_MAX_DEPTH,
    PERM_SUBDIR_FANOUT,
    PERM_SUBDIR_NODES,
    _PERM_REPLIED,
    _QUESTION_REPLIED,
    _open_session_db,
    _perm_action,
    _perm_json,
    _perm_request,
    _pick_answer,
    desktop_permission_sweep,
    desktop_question_sweep,
    desktop_sidecar,
    perm_root_of,
    perm_session_allowed,
    permission_directories,
    permission_event_watcher,
    record_permission_accept,
)


if __name__ == "__main__":
    code = main()
    sys.exit(code)