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
        "You replied STOP: DONE. Before confirming, check your plan once more: "
        "is every step complete and verified? If anything is still open, say "
        "what remains and continue working on it. Otherwise reply STOP: DONE "
        "again."
    ),
    "confirm_install_cli": False,
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
    if os.name != "nt":
        return hawk_linux.desktop_sidecar()
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
        self.posix = os.name != "nt"
        if self.posix:
            return  # psutil-backed (hawk_linux.process_times)
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
        if self.posix:
            return hawk_linux.process_times(pid)
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
        self.posix = os.name != "nt"
        if self.posix:
            return  # psutil-backed (hawk_linux.process_mem_io)
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
        if self.posix:
            return hawk_linux.process_mem_io(pid)
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
    if os.name != "nt":
        return hawk_linux.find_pid("llama-server")
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
    bat = (cfg.get("llama_bat_path") or "").strip()
    if not bat:
        log("LLAMA-DOWN: llama_bat_path is not set; cannot respawn llama-server")
        return False
    log("LLAMA-DOWN: respawning via %s" % bat)
    try:
        from . import llama_restart as _lr
        log_fn = _lr.LOG_FN
    except Exception:
        log_fn = "llama-server.out.log"
    log_path = home_dir() / log_fn
    try:
        fh = open(log_path, "ab")
    except Exception:
        fh = None
    try:
        if os.name == "nt":
            argv = ["cmd", "/c", bat]
        else:
            argv = [bat] if os.access(bat, os.X_OK) else ["sh", bat]
        proc = subprocess.Popen(
            argv,
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
    if os.name != "nt":
        return hawk_linux.gpu_engine_max()
    _ensure_gpu_sampler()
    return _PDH_SAMPLE.get("val", 0.0)


_PDH_SAMPLE = {"val": 0.0, "vram_mb": None, "gpu_inst": [], "gpu_eng": [],
               "disk_inst": [], "t": 0.0}


def gpu_adapters_usage_mb() -> list:
    """Per-adapter dedicated GPU VRAM in use (MB): [(luid, mb)] in WDDM
    enumeration order — the same order the dashboard matches to its
    registry-derived adapter totals. [] when the counter is unavailable."""
    if os.name != "nt":
        return [(g["id"], g["used_mb"]) for g in hawk_linux.gpus()]
    _ensure_gpu_sampler()
    return list(_PDH_SAMPLE.get("gpu_inst") or [])


def gpu_adapters_engine_pct() -> list:
    """Per-adapter GPU PROCESSING utilisation %: [(luid, pct)] where pct is
    the adapter's busiest engine (3D/Copy/VideoEncode/VideoDecode) — the same
    semantics as the all-engine gauge but scoped per adapter. Same LUID order
    as gpu_adapters_usage_mb(). [] when the counter is unavailable."""
    if os.name != "nt":
        return [(g["id"], g["pct"]) for g in hawk_linux.gpus()]
    _ensure_gpu_sampler()
    return list(_PDH_SAMPLE.get("gpu_eng") or [])


def physical_disk_active_pct() -> list:
    """Real-time disk activity % per physical disk: [(instance, pct)] where
    pct = 100 - %Idle Time (Task-Manager "Active time" semantics). Instances
    are plain disk indexes (e.g. "0", "1"); the _Total row is dropped. [] when
    the disk counter is absent (rare failure). On Linux the instances are
    disk names (e.g. "nvme0n1") instead of indexes."""
    if os.name != "nt":
        return hawk_linux.disk_active_pct()
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
    counter is unavailable (engine-only PDH mode)."""
    if os.name != "nt":
        return hawk_linux.gpu_vram_used_mb()
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


def gpu_adapter_totals() -> list[dict]:
    """[{name, total_mb}] — one entry per adapter in the video-class registry
    (WDDM enumeration order): DriverDesc from each subkey, total_mb from the
    QWORD 'HardwareInformation.qwMemorySize' (the real dedicated VRAM; WMI
    AdapterRAM is 32-bit capped), 0 for UMA/virtual adapters without one. This
    mirrors the per-adapter PDH engine rows one-for-one, so the dashboard can
    pair names/totals positionally. On Linux: one entry per GPU from
    hawk_linux.gpus(), in the same order as gpu_adapters_usage_mb(). Empty on
    failure."""
    out: list[dict] = []
    if os.name != "nt":
        return [{"name": g["name"], "total_mb": g["total_mb"]}
                for g in hawk_linux.gpus()]
    try:
        cmd = ('powershell.exe -NoProfile -NonInteractive -Command '
               '"Get-ChildItem \'HKLM:\\SYSTEM\\CurrentControlSet\\Control\\'
               'Class\\{4d36e968-e325-11ce-bfc1-08002be10318}\' | '
               'Where-Object { $_.PSChildName -match \'^\\d{4}$\' } | '
               'ForEach-Object { $p = Get-ItemProperty $_.PSPath; '
               '$v = $p.\'HardwareInformation.qwMemorySize\'; '
               '$s = 0; if ($null -ne $v) { $s = $v }; '
               '\'{0}|{1}\' -f $p.DriverDesc, $s }"')
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                             creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                             timeout=10).stdout
        for ln in res.splitlines():
            ln = ln.strip()
            if "|" not in ln:
                continue
            name, _, size = ln.rpartition("|")
            try:
                mb = int(size) / (1024.0 * 1024.0)
            except ValueError:
                continue
            out.append({"name": name.strip(), "total_mb": mb})
    except Exception:
        return []
    return out


def gpu_vram_total_mb() -> float:
    """Total GPU VRAM (MB) for pinning the VRAM chart. The config override
    (default 16384) wins when > 0; when set to 0, try DXGI auto-detect; else the
    default. Cached — the total does not change at runtime."""
    if _GVRAM["mb"] is None:
        val = float(load_config().get("gpu_vram_total_mb", 0) or 0)
        if val > 0:
            _GVRAM["mb"] = val
        else:
            if os.name == "nt":
                auto = _dxgi_dedicated_vram_mb() or max(
                    (g["total_mb"] for g in gpu_adapter_totals()), default=0) or None
            else:
                auto = hawk_linux.gpu_vram_total_mb()
            # 16 GB only pins the chart axis when no GPU could be detected
            _GVRAM["mb"] = float(auto) if auto else 16384.0
    return float(_GVRAM["mb"])


def system_ram_total_mb() -> float:
    """Total physical RAM (MB) via GlobalMemoryStatusEx. 0 when unavailable."""
    if _SRAM["mb"] is None and os.name != "nt":
        _SRAM["mb"] = hawk_linux.ram_total_mb()
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
            return hawk_linux.ram_used_mb()
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
        path = write_plan_done(session, project_dir, reason, rule_hits, a, cfg)
        log("DONE: build plan complete -> %s" % path)
        if cfg.get("record_escalations_to_session", True):
            db = db_path()
            note = build_plan_done_note(session, reason, rule_hits, str(path))
            if append_session_note(db, session["id"], note, _model_field(session)):
                log("DONE: recorded in session transcript")
        try:
            from . import notify
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


if __name__ == "__main__":
    code = main()
    sys.exit(code)