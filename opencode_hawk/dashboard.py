#!/usr/bin/env python3
"""Hawk dashboard: localhost read-mostly HTTP UI for the coordinator
hawk. Stdlib only. Binds 127.0.0.1."""
from __future__ import annotations
import argparse
import base64
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from . import coordinator  # same dir; load_state/load_config/connect_db/db_path/git_head/...
from . import hawk_linux

DEFAULT_PORT = 8765
WEB_DIR = Path(__file__).resolve().parent / "web"
MAX_BODY_BYTES = 1 << 20  # 1 MiB cap on POST bodies (localhost DoS guard)


def utcms() -> int:
    return int(time.time() * 1000)


# ── control write (the ONLY write path) ────────────────────────────────────────
# Note: user-editable hawk timings (poll_minutes / idle_minutes /
# stalled_turn_minutes) are written via /api/settings to config.json — the
# second, explicit write path the user approved. Everything else stays read-only.
def control_path() -> Path:
    return coordinator.home_dir() / "control.json"


def read_control_seq() -> int:
    try:
        d = json.loads(control_path().read_text("utf-8-sig"))
        return int(d.get("seq", 0))
    except Exception:
        return 0


def write_control(intent: str) -> dict:
    d = {"intent": intent, "seq": read_control_seq() + 1, "at": utcms()}
    coordinator.save_json(control_path(), d)
    return d


# ── hawk timing settings (user-adjustable, persisted to config.json) ────────
SETTINGS_SPEC = {
    "poll_minutes":            {"label": "Poll interval (min)",     "min": 1, "max": 60},
    "idle_minutes":            {"label": "Worker step-in (min)",    "min": 1, "max": 720},
    "stalled_turn_minutes":    {"label": "Stopped-turn trigger (min)", "min": 1, "max": 720},
    "re_stall_minutes":        {"label": "Self-check repeat gap (min)", "min": 1, "max": 720},
    "stall_llama_window_s":    {"label": "Llama busy window (s)",   "min": 30, "max": 3600},
    "llama_gpu_cache_s":       {"label": "GPU sample cache (s)",    "min": 1, "max": 120},
    "llama_kill_degraded":     {"label": "Kill llama when degraded", "type": "bool"},
    "llama_kill_degraded_s":   {"label": "Sustained degradation before kill (s)", "type": "int", "min": 30, "max": 3600},
    "llama_kill_tps":          {"label": "Kill when avg tps below (t/s)", "type": "float", "min": 1.0, "max": 60.0, "step": 0.5},
}


def _setting_fallback(k: str):
    """Default value for a settings key before it appears in config.json."""
    if SETTINGS_SPEC[k].get("type") == "bool":
        return False
    if k in ("llama_kill_degraded_s", "llama_kill_tps"):
        try:
            from . import llama_restart
            return llama_restart.DEFAULTS.get(k)
        except Exception:
            return None
    return coordinator.CONFIG_DEFAULTS.get(k, 0)


def settings_defaults() -> dict:
    cfg = coordinator.load_config()
    out = {}
    for k, spec in SETTINGS_SPEC.items():
        typ = spec.get("type", "int")
        v = cfg.get(k, _setting_fallback(k))
        try:
            if typ == "bool":
                out[k] = v if isinstance(v, bool) else (str(v).strip().lower() in ("1", "true", "yes", "on"))
            elif typ == "float":
                out[k] = float(v)
            else:
                out[k] = int(v)
        except (TypeError, ValueError):
            out[k] = spec.get("min", 0)
    return out


def update_settings(payload: dict) -> dict:
    """Merge validated settings keys into config.json (atomic). Returns the
    keys actually updated. Unknown keys are ignored; values are clamped."""
    cfg = coordinator.load_config()
    updated = {}
    for k, spec in SETTINGS_SPEC.items():
        if k not in payload:
            continue
        typ = spec.get("type", "int")
        if typ == "bool":
            raw = str(payload[k]).strip().lower()
            if raw in ("1", "true", "yes", "on"):
                v = True
            elif raw in ("0", "false", "no", "off"):
                v = False
            else:
                raise ValueError("%s must be yes or no" % k)
        else:
            try:
                v = float(payload[k]) if typ == "float" else int(payload[k])
            except (TypeError, ValueError):
                raise ValueError("%s must be a number" % k)
            v = max(spec["min"], min(spec["max"], v))
            v = round(v, 2) if typ == "float" else int(v)
        cfg[k] = v
        updated[k] = v
    if updated:
        coordinator.save_json(coordinator.home_dir() / "config.json", cfg)
    return updated


# ── log tail ──────────────────────────────────────────────────────────────────
def log_path() -> Path:
    return coordinator.home_dir() / "monitor.log"


def tail_last_log_line() -> str:
    try:
        p = log_path()
        if p.exists():
            with open(p, "rb") as f:
                f.seek(max(0, p.stat().st_size - 16384))
                return f.read().decode("utf-8", "replace").strip().splitlines()[-1] or ""
    except Exception:
        pass
    return ""


def creation_flags() -> int:
    """Prevent flashing console windows for subprocess children (the dashboard
    itself runs with CREATE_NO_WINDOW, so console children get their own
    window unless we suppress it)."""
    if os.name == "nt":
        return (subprocess.CREATE_NO_WINDOW
                | getattr(subprocess, "DETACHED_PROCESS", 0x00000008)
                | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200))
    return 0


# ── git timeline ──────────────────────────────────────────────────────────────
_git_log_cache = {}
_git_branch_cache = {}
_GIT_LOG_TTL_S = 15


def clean_title(s: str, maxlen: int = 110) -> str:
    s = re.sub(r"\s+", " ", s.strip().lstrip("-* ").strip())
    if len(s) > maxlen:
        return s[: maxlen - 1].rstrip() + "…"
    return s


def short(s: str, maxlen: int = 80) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip())
    if len(s) > maxlen:
        return s[: maxlen - 1].rstrip() + "…"
    return s


def stop_title(body: str, kind: str = "DONE") -> str:
    """Derive a human-readable milestone title from the STOP message body.
    Prefers the '## Objective' bullet, falls back to the first content line."""
    lines = [l.strip() for l in body.splitlines()]
    for i, l in enumerate(lines):
        if l.lower().startswith("## objective"):
            for l2 in lines[i + 1:]:
                if l2.startswith(("-", "*")):
                    return clean_title(l2)
                if l2:
                    break
            break
    for l in lines:
        if not l or l.startswith("#") or "STOP:" in l or "RESULT:" in l:
            continue
        return clean_title(l)
    return "STOP: " + kind


def _esc_reason(txt: str) -> str:
    """Pull the '- Reason:' line from an escalation file (fallback: first
    content line that is not a heading / meta line)."""
    for l in txt.splitlines():
        s = l.strip()
        if s.startswith("- Reason:"):
            return s[len("- Reason:"):].strip()
    for l in txt.splitlines():
        s = l.strip()
        if s and not s.startswith("#") and not s.startswith("- Time:") \
           and not s.startswith("- Session:"):
            return s
    return ""


_MD_PATTERNS = [
    (re.compile(r"\*\*([^*\n]+?)\*\*"), r"\1"),            # **bold**
    (re.compile(r"(?<!\w)\*([^\s*][^*\n]*?)\*(?!\w)"), r"\1"),  # *italic*
    (re.compile(r"__([^_\n]+?)__"), r"\1"),                # __bold__
    (re.compile(r"`([^`\n]+?)`"), r"\1"),                  # `code`
    (re.compile(r"^\s{0,3}#{1,6}\s*", re.M), ""),          # "# heading"
]


def strip_md(s: str) -> str:
    """Drop inline markdown emphasis so feed/tooltip text reads as plain text."""
    if not s or not isinstance(s, str):
        return s
    for pat, rep in _MD_PATTERNS:
        s = pat.sub(rep, s)
    return s.replace("**", "")


def git_branch(project_dir: str) -> str:
    now = time.time()
    c = _git_branch_cache.get(project_dir)
    if c and now - c[0] < _GIT_LOG_TTL_S:
        return c[1]
    try:
        out = subprocess.run(["git", "-C", project_dir, "branch", "--show-current"],
                             capture_output=True, text=True, encoding="utf-8",
                             errors="replace", timeout=10,
                             creationflags=creation_flags()).stdout.strip()
        _git_branch_cache[project_dir] = (now, out)
        return out
    except Exception:
        return ""


def git_log_events(project_dir: str, n: int = 50) -> list[dict]:
    key = (project_dir, n)
    now = time.time()
    cached = _git_log_cache.get(key)
    if cached and now - cached[0] < _GIT_LOG_TTL_S:
        return cached[1]
    try:
        out = subprocess.run(
            ["git", "-C", project_dir, "log", "--format=%H|%ct|%s", "-n", str(n)],
            capture_output=True, text=True, encoding="utf-8",
            errors="replace", timeout=15,
            creationflags=creation_flags()).stdout
        evs = []
        for line in out.splitlines():
            parts = line.split("|", 2)
            if len(parts) != 3:
                continue
            sha, ts, subj = parts
            evs.append({"kind": "commit", "id": sha[:8], "time": int(ts) * 1000,
                        "title": short(subj, 90), "detail": subj, "ok": True})
        _git_log_cache[key] = (now, evs)
        return evs
    except Exception:
        return []


def milestone_events(con, session_id: str, since_ms: int = 0) -> list[dict]:
    """Model-reported build milestones. A milestone is an ASSISTANT message
    whose FINAL content line carries the STOP marker — the literal end-of-turn
    signal. Prose that merely QUOTES the marker mid-message (diagnosis notes,
    hawk escalation summaries that quote a STOP line, decision write-ups)
    used to leak in as false milestones because the marker was searched
    anywhere in the body; it is now anchored to the last line so those are
    dropped. Markers: STOP: DONE | NEEDS_DECISION | BLOCKED; the older
    STOP: VERIFIED still counts as a completed milestone.

    An empty `session_id` aggregates across ALL sessions (the web frontend's
    "All sessions" view). `since_ms` narrows the scan to recent parts (the
    timeline UI window); 0 = all time.
    """
    try:
        rows = con.execute(
            "SELECT p.session_id, p.message_id, p.time_created, p.data, m.data FROM part p "
            "JOIN message m ON m.id = p.message_id "
            "WHERE (? = '' OR p.session_id=?) AND p.time_created >= ? "
            "AND json_extract(p.data, '$.type') "
            "IN ('text','assistant') ORDER BY p.time_created",
            (session_id, session_id, since_ms)).fetchall()
    except Exception:
        return []
    msgs: dict[str, list] = {}
    for psess, pid, tc, pdata, mdata in rows:
        d = coordinator.parse_part_data(pdata)
        if not isinstance(d, dict):
            continue
        body = str(d.get("text", ""))
        role = ""
        try:
            role = json.loads(mdata).get("role") if mdata else ""
        except Exception:
            pass
        e = msgs.setdefault(pid, [role, "", 0, psess])
        e[1] += body + "\n"
        if int(tc) > e[2]:
            e[2] = int(tc)
    evs = []
    RESULT_LINE = re.compile(r"^RESULT:\s*PASS(?:\s+tests=\d+)?\s*$")
    LAST_STOP = re.compile(
        r"^STOP:\s*(DONE|VERIFIED|NEEDS_DECISION|BLOCKED)(?:\s+\S+)*$|"
        r"STOP:\s*(DONE|VERIFIED|NEEDS_DECISION|BLOCKED)\s*$")
    for mid, (role, body, tc, psess) in msgs.items():
        if role != "assistant":
            continue
        body = body.strip()
        if not body or re.sub(r'^[\s"]+', "", body).startswith(("[coordinator]", "[hawk")):
            continue
        lines = body.splitlines()
        n = len(lines)
        while n and RESULT_LINE.match(lines[n - 1]):  # marker may sit above a trailing tests line
            n -= 1
        if n <= 0:
            continue
        m = LAST_STOP.search(lines[n - 1])
        if not m:
            continue
        kind = m.group(m.lastindex)  # the alternative that actually matched
        tests = None
        rm = re.search(r"RESULT:\s*PASS\s+tests=(\d+)", body)
        if rm:
            tests = int(rm.group(1))
        evs.append({"kind": "milestone", "id": "%s-%d" % (kind, tc), "mid": mid,
                    "time": tc, "sid": psess or "",
                    "title": stop_title(body, kind),
                    "detail": body.strip()[:220], "ok": kind in ("DONE", "VERIFIED"), "tests": tests})
    return evs


def _esc_session(txt: str) -> str:
    """Session id from an escalation markdown's '- Session: <sid> ...' line."""
    for l in txt.splitlines():
        ls = l.strip()
        if ls.startswith("- Session:"):
            rest = ls[len("- Session:"):].strip()
            return (rest.split()[0] if rest else "").strip()
    return ""


def escalation_events(directory: str) -> list[dict]:
    d = Path(directory)
    evs = []
    if d.exists():
        for p in sorted(d.glob("*.md")):
            try:
                txt = p.read_text("utf-8", errors="replace")
                reason = _esc_reason(txt)
                is_done = p.stem.startswith("done_")
                evs.append({"kind": "plan_done" if is_done else "escalation",
                            "id": p.stem, "time": int(p.stat().st_mtime * 1000),
                            "sid": _esc_session(txt),
                            "title": reason or p.stem, "detail": txt.strip()[:300],
                            "ok": is_done, "reason": reason})
            except Exception:
                continue
    return evs


def continue_events(con, session_id: str, since_ms: int = 0) -> list[dict]:
    """Hawk injections (auto-continues / done-confirmations / todo-nudges)
    recorded in the session transcript: user text parts prefixed
    '[coordinator]' (legacy) or equal to the configured continue/confirm
    message (current hawk prompts no longer carry the marker). Distinct from a
    milestone (that is the model reporting; this is the hawk acting).
    An empty `session_id` aggregates across ALL sessions."""
    try:
        rows = con.execute(
            "SELECT p.session_id, p.id, p.time_created, p.data FROM part p "
            "JOIN message m ON m.id = p.message_id "
            "WHERE (? = '' OR p.session_id=?) AND p.time_created >= ? "
            "AND json_extract(m.data, '$.role')='user' "
            "AND json_extract(p.data, '$.type')='text' ORDER BY p.time_created",
            (session_id, session_id, since_ms)).fetchall()
    except Exception:
        return []
    try:
        cfg = coordinator.load_config()
    except Exception:
        cfg = {}
    cont = str(cfg.get("continue_message") or "").strip()
    done = str(cfg.get("done_confirm_message") or "").strip()
    # A distinctive opening shared by the hawk prompts (and by any truncated
    # variant the CLI may write). Both continue and done messages start the
    # same way; they split after the grant sentence.
    hawk_prefix = cont[:40] if len(cont) >= 40 else cont
    evs = []
    for psess, pid, tc, data in rows:
        d = coordinator.parse_part_data(data)
        if not isinstance(d, dict):
            continue
        body = str(d.get("text", "")).strip()
        # Injections can arrive double-quoted ("..."), e.g. via the desktop
        # sidecar's CLI launch; unwrap before classifying.
        if len(body) >= 2 and body[0] == '"' and body[-1] == '"':
            body = body[1:-1].strip()
        hit = None
        if body.startswith("[coordinator]"):
            hit = "[coordinator]"
        elif hawk_prefix and body.startswith(hawk_prefix):
            hit = "done_confirm_message" if (done and body.startswith(done)) \
                else "continue_message"
        if not hit:
            continue
        if hit == "done_confirm_message":
            kind = "confirm_done"
        elif "unchecked task" in body:
            kind = "nudge"
        else:
            kind = "continue"
        title = short(re.sub(r"^\[coordinator\]\s*", "", body), 90)
        evs.append({"kind": kind, "id": str(pid)[:8], "mid": pid,
                    "time": int(tc), "sid": psess or "",
                    "title": title,
                    "detail": body.strip()[:220], "ok": True})
    return evs


def session_compaction_events(con, session_id: str) -> list[dict]:
    """Session-level auto-compactions: opencode compresses the conversation
    transcript when it grows (part data type='compaction'). These are the
    compactions a user actually sees regularly; llama's KV-cache compaction
    (telemetry compact flag) is a separate, rarer signal at big n_ctx."""
    try:
        rows = con.execute(
            "SELECT time_created FROM part WHERE session_id=? "
            "AND json_extract(data, '$.type')='compaction' ORDER BY time_created",
            (session_id,)).fetchall()
        return [{"t": int(r[0])} for r in rows]
    except Exception:
        return []


def permission_events() -> list[dict]:
    """Desktop storage-location auto-accepts the coordinator replied 'always'
    to (JSONL in the coordinator home dir). This is a timeline event source
    because the asks otherwise only reach the monitor log, not the Event list:
    'It reports correct as auto-accept, but doesn't list it in the web
    interface Event list.'"""
    p = coordinator.home_dir() / "permission_events.jsonl"
    if not p.exists():
        return []
    evs = []
    try:
        lines = p.read_text("utf-8", errors="replace").splitlines()[-100:]
    except OSError:
        return []
    for ln in lines:
        try:
            d = json.loads(ln)
        except Exception:
            continue
        if d.get("kind") != "permission_accept":
            continue
        target = str(d.get("target") or "")
        sess = str(d.get("session") or "")
        title = "permission auto-accept: %s" % short(target.split(",")[0], 60)
        if sess:
            title += " · %s" % short(sess, 12)
        evs.append({"kind": "permission_accept", "id": str(d.get("id") or "")[:12],
                    "time": int(d.get("t") or 0), "sid": sess,
                    "title": title, "detail": (target or title)[:220], "ok": True})
    return evs


def _clock(ms: int) -> str:
    """Wall-clock HH:MM:SS for an epoch-ms instant, in LOCAL time (the times
    the user actually sees; UTC was rendering 2h off on CEST machines)."""
    return time.strftime("%H:%M:%S", time.localtime(ms / 1000)) if ms else "?"


def subagent_events(con, session_id: str, since_ms: int = 0) -> list[dict]:
    """Task-tool subagent launches in this session: type, start/end times,
    duration, and the child session's token usage. Subagent work happens in
    child sessions, so the root's Event tab would otherwise show no trace of
    it at all. An empty `session_id` aggregates across ALL sessions.

    Provider-truth (design 2026-09-23): every event carries `state`
    ('started'|'finished'), `tag` ('started'|'finished'|'error'; error counts
    as finished - never renders as running) and `src` ('llama' when the task
    registry owned it, else 'db'). Finished walls come from the llama release
    line when available; opencode structure only decides WHEN the state flips
    (tool-loop caveat), never how long it ran. A started row's end shows live
    `now`, so elapsed time keeps updating from the task stream."""
    try:
        rows = con.execute(
            "SELECT session_id, time_created, id, data FROM part "
            "WHERE (? = '' OR session_id=?) AND time_created >= ? "
            "AND json_extract(data, '$.type')='tool' "
            "AND json_extract(data, '$.tool')='task' ORDER BY time_created",
            (session_id, session_id, since_ms)).fetchall()
    except Exception:
        return []
    try:
        _llama_assign_owners(con, utcms())
    except Exception:
        pass
    evs = []
    for psess, tc, pid, d in rows:
        try:
            pd = json.loads(d)
        except Exception:
            continue
        st = pd.get("state") or {}
        t = st.get("time") or {}
        inp = st.get("input") or {}
        started, ended = int(t.get("start") or 0), int(t.get("end") or 0)
        p_status = str(st.get("status") or "")
        meta = st.get("metadata") or {}
        child = str(meta.get("sessionId") or "")
        if not child:                       # legacy parts: regex the output
            sid_m = re.search(r'id="([^"]+)"', str(st.get("output") or ""))
            child = sid_m.group(1) if sid_m else ""
        tokens = 0
        if child:
            try:
                # tokens_output (generation) only — tokens_input counts the
                # full context at each message (all prior turns), so summing
                # it inflates the total ~5-7x vs the actual tokens processed.
                r = con.execute(
                    "SELECT COALESCE(tokens_output,0) FROM session WHERE id=?",
                    (child,)).fetchone()
                tokens = int(r[0] or 0) if r else 0
            except Exception:
                tokens = 0
        atype = str(inp.get("subagent_type") or "subagent")
        title = "subagent %s: %s" % (atype, short(str(inp.get("description") or ""), 48))
        owner = "part:" + pid
        try:
            state, tag, src = _llama_owner_state(
                con, owner, child, p_status, started, utcms())
        except Exception:
            state, tag, src = "started", "started", "db"
        owned = _llama_owned_tasks(owner)
        tps = max([o.get("tps") or 0 for o in owned] or [0])
        last_end = max([o.get("end") or 0 for o in owned] or [0])
        if state == "finished":
            # Real end wall: part's own time.end, else the last llama task
            # it owned, else INDEPENDENT child terminal evidence (last
            # activity of the child session - a part cut off by an
            # interrupted shutdown never got time.end, and using the part's
            # time_created here mints end-before-start rows), else the part
            # creation wall as a last resort.
            end_show = (max(ended, last_end)
                        or _llama_child_last_activity(con, child)
                        or int(tc))
        else:
            end_show = utcms()              # live elapsed from the task stream
        if not started:
            started = (min([o.get("start") or 0 for o in owned] or [0])
                       or end_show)
        if state == "started":
            detail = _clock(started)
            if tps:
                detail += " · %.1f t/s" % tps
        else:
            detail = "%s -> %s" % (_clock(started), _clock(end_show))
            ds = max(0, end_show - started) / 1000
            if ds > 0:
                detail += " · " + ("%.0fs" % ds if ds < 90 else "%.1fmin" % (ds / 60))
            if tokens:
                detail += " · %d tok" % tokens
        evs.append({"kind": "subagent", "id": str(pid)[:8], "mid": pid,
                    "time": end_show if state == "finished" else started,
                    "sid": psess or "", "title": title, "detail": detail,
                    "atype": str(inp.get("subagent_type") or ""),
                    "ok": tag == "finished", "start": started, "end": end_show,
                    "tokens": tokens, "child": child,
                    "state": state, "tag": tag, "src": src, "tps": tps})
    return evs


def timeline_events(project_dir: str, session_id: str, con,
                    since_ms: int = 0) -> list[dict]:
    # con is None when there is no opencode DB yet (fresh install): the
    # file-backed events (commits, escalations, permissions) still show.
    evs = (git_log_events(project_dir)
           + escalation_events(str(coordinator.home_dir() / "escalations"))
           + permission_events())
    if con is not None:
        evs += (milestone_events(con, session_id, since_ms=since_ms)
                + continue_events(con, session_id, since_ms=since_ms)
                + subagent_events(con, session_id, since_ms=since_ms))
    evs.sort(key=lambda e: e["time"], reverse=True)
    for e in evs:
        for k in ("title", "detail", "reason"):
            if e.get(k):
                e[k] = strip_md(e[k])
    # Every event says which session it came from: resolve a display label
    # from the session table (title, else a short id tail). Child sessions
    # inherit their root's title via the parent link where the record exists.
    titles: dict[str, str] = {}
    try:
        rows = con.execute(
            "SELECT s.id, s.title, COALESCE(p.title, '') FROM session s "
            "LEFT JOIN session p ON p.id = s.parent_id").fetchall()
    except Exception:
        rows = []
    for sid, st, pt in rows:
        t = str(st or "").strip() or str(pt or "").strip()
        if t:
            titles[sid] = t
    proj_base = (project_dir or "").replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]
    for e in evs:
        sid = e.get("sid") or ""
        if sid:
            t = titles.get(sid) or ""
            e["sess"] = (t[:32] + "…") if len(t) > 32 else t
            if not e["sess"]:
                e["sess"] = "…" + sid[-8:]
        elif e["kind"] == "commit":
            # Commits are repo-scoped, not session-scoped — say which repo.
            e["sid"] = ""
            e["sess"] = ("repo " + proj_base) if proj_base else ""
        else:
            e["sid"] = ""
            e["sess"] = ""
    return evs[:200]


def escalations_list(session_id: str | None = None) -> list[dict]:
    d = coordinator.home_dir() / "escalations"
    out = []
    if d.exists():
        for p in sorted(d.glob("*.md"), reverse=True):
            try:
                txt = p.read_text("utf-8", errors="replace")
                if session_id:
                    saw_line = False
                    match = False
                    for l in txt.splitlines():
                        ls = l.strip()
                        if ls.startswith("- Session:"):
                            saw_line = True
                            match = session_id in ls
                            break
                    if saw_line:
                        if not match:
                            continue
                    elif not p.stem.endswith("_%s" % session_id[-6:]):
                        continue
                reason = _esc_reason(txt)
                if not reason:
                    reason = p.stem
                out.append({"file": p.name, "title": reason,
                            "time": int(p.stat().st_mtime * 1000),
                            "reason": reason, "detail": txt.strip()[:800]})
            except Exception:
                continue
    for e in out:
        for k in ("title", "detail", "reason"):
            if e.get(k):
                e[k] = strip_md(e[k])
    return out


def session_title(con, session_id: str) -> str:
    try:
        r = con.execute("SELECT title FROM session WHERE id=?", (session_id,)).fetchone()
        return (r[0] or "") if r else ""
    except Exception:
        return ""


_SESSION_TOGGLE_KEYS = ("enabled", "auto_accept", "auto_continue")


def _default_session_settings():
    return {"enabled": False, "auto_accept": True, "auto_continue": True, "project_dir": ""}


def provider_of(model_json) -> tuple[str, str]:
    try:
        mj = json.loads(model_json) if isinstance(model_json, str) else model_json
        if isinstance(mj, dict):
            return str(mj.get("providerID") or ""), str(mj.get("id") or "")
    except Exception:
        pass
    return "", ""


def recommended_session_id(con, cfg) -> str:
    """Session the auto-chooser would pick: newest llama.cpp session inside
    `project_dir` that has at least one message (mirrors coordinator's
    auto-detect branch in select_session). Empty string when none."""
    proj = (cfg.get("project_dir") or "").lower().replace("\\", "/").rstrip("/")
    rows = con.execute(
        "SELECT id, model, directory FROM session ORDER BY time_updated DESC"
    ).fetchall()
    for sid, model, directory in rows:
        prov, _ = provider_of(model)
        if prov != "llama.cpp":
            continue
        d = (directory or "").lower().replace("\\", "/").rstrip("/")
        if proj and d and d != proj:
            continue
        try:
            n = con.execute(
                "SELECT COUNT(*) FROM message WHERE session_id=?", (sid,)
            ).fetchone()[0]
        except Exception:
            n = 1  # table absent (test in-memory DB) — assume populated
        if n == 0:
            continue
        return sid
    return ""


# A session counts as "live" (currently open/active) when it had activity
# within this window. Overridable per config via `session_live_minutes`.
SESSION_LIVE_MINUTES = 10


def _is_live(tu, now: int, window_ms: int) -> bool:
    return bool(tu) and (now - int(tu)) < window_ms


def sessions_list_payload(show_all: bool = False) -> list[dict]:
    """Session list for the dashboard card.

    By default only "live" sessions (activity within the live window) and
    explicitly-enabled ones are returned; `show_all=True` returns everything
    (the "show all" escape hatch).

    Subagent (child) sessions are grouped directly UNDER their main session:
    parents keep the llama-first / recency sort, each child sits right after
    its parent (newest first, deeper chains nest the same way). Children whose
    parent is not in the displayed set stay visible at the end. Every entry
    carries a `depth` (0 = main session, 1 = direct child, ...) so the UI can
    indent the tree."""
    cfg = coordinator.load_config()
    ms = cfg.get("monitored_sessions") or {}
    live_window_ms = int(cfg.get("session_live_minutes", SESSION_LIVE_MINUTES)) * 60_000
    now = utcms()
    db = coordinator.db_path()
    rows = []
    if db.exists():
        con = coordinator.connect_db(db)
        try:
            rows = con.execute(
                "SELECT id, title, model, directory, parent_id, "
                "time_created, time_updated FROM session "
                "ORDER BY time_updated DESC").fetchall()
        finally:
            con.close()
    out = []
    rec = ""
    if db.exists():
        con = coordinator.connect_db(db)
        try:
            rec = recommended_session_id(con, cfg)
        finally:
            con.close()
    # One read-only connection for the subworker lifecycle lookups below;
    # correlate the payload's llama task owners once, not per row.
    tcon = None
    try:
        if db.exists():
            tcon = coordinator.connect_db(db)
            _llama_assign_owners(tcon, utcms())
    except Exception:
        tcon = None
    for r in rows:
        sid, title, model, directory, parent_id, tc, tu = r
        final = dict(_default_session_settings())
        final.update(ms.get(sid) or {})
        live = _is_live(tu, now, live_window_ms)
        if not (show_all or final.get("enabled") or live):
            continue
        # Subworker rows carry the same provider-truth lifecycle (design
        # 2026-09-23): session-kind owners were correlated above (once), so
        # this is a pure state-machine read per child.
        sub_state = sub_tag = sub_src = ""
        if parent_id and tcon is not None:
            try:
                sub_state, sub_tag, sub_src = _llama_owner_state(
                    tcon, "session:" + sid, "", "", int(tc or 0), utcms())
            except Exception:
                sub_state, sub_tag, sub_src = "started", "started", "db"
        provider, model_id = provider_of(model)
        out.append({"id": sid, "title": title or "", "dir": directory or "",
                    "provider": provider, "model": model_id,
                    "parent_id": parent_id or "", "time_updated": tu,
                    "is_subworker": bool(parent_id), "live": live, "depth": 0,
                    "recommended": bool(rec) and sid == rec,
                    "settings": final,
                    "sub_state": sub_state, "sub_tag": sub_tag,
                    "sub_src": sub_src})
    out.sort(key=lambda e: (e["provider"] != "llama.cpp", -e["time_updated"]))
    # Re-nest children under their parent (parents only in the sort above).
    by_parent: dict[str, list[dict]] = {}
    parents: list[dict] = []
    for e in out:
        if e["parent_id"]:
            by_parent.setdefault(e["parent_id"], []).append(e)
        else:
            parents.append(e)
    ordered: list[dict] = []
    emitted: set[str] = set()

    def _emit(entry: dict, depth: int) -> None:
        ordered.append(entry)
        emitted.add(entry["id"])
        kids = sorted(by_parent.pop(entry["id"], []),
                      key=lambda k: -k["time_updated"])
        for k in kids:
            if k["id"] in emitted:  # cycle guard against corrupt data
                continue
            k["depth"] = depth + 1
            _emit(k, depth + 1)

    for p in parents:
        _emit(p, 0)
    orphans: list[dict] = []
    for kids in by_parent.values():
        orphans.extend(kids)
    orphans.sort(key=lambda e: -e["time_updated"])
    for e in orphans:
        e["depth"] = 1
    ordered.extend(orphans)
    if tcon is not None:
        try:
            tcon.close()
        except Exception:
            pass
    return ordered


def _token_curve(con, session_id, window_s: int, now_ms: int,
                 buckets: int = 0) -> dict:
    """Cumulative input/output tokens over the window, from opencode's own
    per-message accounting (the exact numbers, no sampling).

    session_id None -> every session. Otherwise the session PLUS all its
    descendant (subagent) sessions, since their llama work is part of it.
    Each assistant message counts at its completion time: input = newly
    processed prompt tokens (tokens.input; cache reads are excluded, they are
    not re-processed), output = tokens.output + tokens.reasoning.
    The curve starts at 0 at the window start and is sampled on `buckets`
    evenly spaced ticks up to now, so the x-axis always spans the selected
    window and the last point equals the window totals."""
    cutoff = now_ms - int(window_s) * 1000
    q = ("SELECT COALESCE(json_extract(m.data,'$.time.completed'), m.time_created),"
         " COALESCE(json_extract(m.data,'$.tokens.input'),0),"
         " COALESCE(json_extract(m.data,'$.tokens.output'),0)"
         " + COALESCE(json_extract(m.data,'$.tokens.reasoning'),0)"
         " FROM message m")
    if session_id:
        q = ("WITH RECURSIVE tree(id) AS (SELECT ? UNION "
             " SELECT s.id FROM session s JOIN tree ON s.parent_id = tree.id) "
             + q + " JOIN tree ON m.session_id = tree.id"
             " WHERE m.time_created >= ? - 86400000"
             " AND json_extract(m.data,'$.role') = 'assistant'")
        args = (session_id, cutoff)
    else:
        q += (" WHERE m.time_created >= ? - 86400000"
              " AND json_extract(m.data,'$.role') = 'assistant'")
        args = (cutoff,)
    # time_created is pre-filtered a day early: a long generation can start
    # before the window yet complete inside it.
    rows = sorted((int(t), int(i or 0), int(o or 0))
                  for t, i, o in con.execute(q, args).fetchall()
                  if t and cutoff <= int(t) <= now_ms)
    buckets = max(2, int(buckets or TELE_TARGET_POINTS))
    step = (now_ms - cutoff) / (buckets - 1)
    pts = []
    cin = cout = 0
    k = 0
    for j in range(buckets):
        t = int(cutoff + j * step) if j < buckets - 1 else now_ms
        while k < len(rows) and rows[k][0] <= t:
            cin += rows[k][1]
            cout += rows[k][2]
            k += 1
        pts.append({"t": t, "tokens_input": cin, "tokens_output": cout})
    return {"points": pts, "total_input": cin, "total_output": cout}


def _token_curve_db(session_id, window_s: int) -> dict:
    try:
        con = coordinator.connect_db(coordinator.db_path())
    except Exception as e:
        coordinator.log_debug("token curve: %s" % e)
        return {"points": [], "total_input": 0, "total_output": 0}
    try:
        return _token_curve(con, session_id, window_s, utcms())
    finally:
        con.close()


def session_token_history(session_id: str, window_s: int) -> dict:
    """Per-session cumulative token curve for a window (session + its
    subagent sessions). See _token_curve."""
    if not session_id:
        return {"points": [], "total_input": 0, "total_output": 0}
    return _token_curve_db(session_id, window_s)


def session_token_history_all(window_s: int) -> dict:
    """'All sessions' cumulative token curve: every session's messages, i.e.
    exactly the per-session curves added together. See _token_curve."""
    return _token_curve_db(None, window_s)


def session_stats_payload(session_id: str) -> dict:
    """Per-session stats: token counts, duration, token rate, context size."""
    sid = str(session_id or "").strip()
    if not sid:
        raise ValueError("session_id required")
    db = coordinator.db_path()
    if not db.exists():
        raise ValueError("opencode DB not found")
    con = coordinator.connect_db(db)
    try:
        row = con.execute(
            "SELECT tokens_input, tokens_output, time_created, time_updated "
            "FROM session WHERE id = ?", (sid,)).fetchone()
    finally:
        con.close()
    if not row:
        raise ValueError("session not found")
    tokens_in, tokens_out, tc, tu = row
    now = utcms()
    duration_ms = max(0, (tu or 0) - (tc or 0))
    duration_s = duration_ms / 1000.0
    token_rate = (tokens_out / duration_s) if duration_s > 0 else 0.0
    last_activity_s = max(0, (now - (tu or 0)) / 1000.0)
    # Context size from the latest telemetry sample (global server snapshot).
    ctx_tokens = 0
    tdb = coordinator.home_dir() / "hawk_telemetry.db"
    if tdb.exists():
        try:
            tcon = coordinator.connect_db(tdb)
            try:
                r = tcon.execute(
                    "SELECT ctx_tokens FROM telemetry ORDER BY ts DESC LIMIT 1"
                ).fetchone()
                ctx_tokens = int(r[0]) if r and r[0] is not None else 0
            finally:
                tcon.close()
        except Exception:
            ctx_tokens = 0
    return {
        "session_id": sid,
        "tokens_input": int(tokens_in or 0),
        "tokens_output": int(tokens_out or 0),
        "duration_s": round(duration_s, 1),
        "token_rate": round(token_rate, 2),
        "last_activity_s": round(last_activity_s, 1),
        "context_tokens": ctx_tokens,
    }


def delete_session(session_id: str) -> dict:
    """Delete a session and all its sub-sessions from the opencode DB
    and from config.json's monitored_sessions."""
    sid = str(session_id or "").strip()
    if not sid:
        raise ValueError("session_id required")
    db = coordinator.db_path()
    if not db.exists():
        raise ValueError("opencode DB not found")
    con = coordinator.connect_db(db)
    try:
        # Collect the session and all descendants (recursive).
        to_delete: list[str] = [sid]
        queue: list[str] = [sid]
        while queue:
            cur = queue.pop(0)
            rows = con.execute(
                "SELECT id FROM session WHERE parent_id = ?", (cur,)).fetchall()
            for r in rows:
                to_delete.append(r[0])
                queue.append(r[0])
        placeholders = ",".join("?" * len(to_delete))
        con.execute(
            "DELETE FROM session WHERE id IN (%s)" % placeholders, to_delete)
        con.commit()
    finally:
        con.close()
    # Remove from config.json monitored_sessions (and any children).
    cfg = coordinator.load_config()
    ms = dict(cfg.get("monitored_sessions") or {})
    removed_cfg = [s for s in to_delete if s in ms]
    for s in removed_cfg:
        ms.pop(s, None)
    cfg["monitored_sessions"] = ms
    # If the deleted session was the selected one, clear it.
    if cfg.get("session_id") == sid:
        cfg["session_id"] = ""
    coordinator.save_json(coordinator.home_dir() / "config.json", cfg)
    return {"deleted": len(to_delete), "config_removed": len(removed_cfg)}


def update_session_settings(payload: dict) -> dict:
    cfg = coordinator.load_config()
    sid = str(payload.get("session_id") or "").strip()
    if not sid:
        raise ValueError("session_id required")
    ms = dict(cfg.get("monitored_sessions") or {})
    if payload.get("remove") is True:
        had = sid in ms
        ms.pop(sid, None)
        cfg["monitored_sessions"] = ms
        sel_changed = ("selected" in payload
                       and (cfg.get("session_id") or "") != str(payload["selected"] or "").strip())
        if sel_changed:
            cfg["session_id"] = str(payload["selected"] or "").strip()
        coordinator.save_json(coordinator.home_dir() / "config.json", cfg)
        return {"changed": had or sel_changed}
    entry = dict(ms.get(sid) or _default_session_settings())
    for k in _SESSION_TOGGLE_KEYS:
        if k in payload:
            entry[k] = bool(payload[k])
    if isinstance(payload.get("project_dir"), str):
        entry["project_dir"] = payload["project_dir"].strip()
    ms[sid] = entry
    prev = cfg.get("monitored_sessions") or {}
    cfg["monitored_sessions"] = ms
    if "selected" in payload:
        cfg["session_id"] = str(payload["selected"] or "").strip()
    coordinator.save_json(coordinator.home_dir() / "config.json", cfg)
    return {"changed": ms != prev}


_ntfy_channel_cache = {"t": 0.0, "v": []}


def ntfy_payload() -> dict:
    from . import notify
    import time as _t
    cfg = coordinator.load_config()
    n = notify._ntfy_cfg(cfg)
    # The channel (what is ACTUALLY on the topic right now, from ntfy's
    # pollable history) is refreshed at most every 30s: ntfy.sh allows a
    # client ~12 requests/min and the monitor's reply poll already spends
    # half of that; polling faster starved deletes into HTTP 429. Deletes
    # update this cache directly, so the list still reacts instantly.
    # On a transient fetch failure the last good snapshot is kept.
    if _t.time() - _ntfy_channel_cache["t"] > 30:
        try:
            _ntfy_channel_cache.update(t=_t.time(), v=notify.ntfy_channel_state(cfg))
        except Exception:
            pass  # stale-but-last-good stays in the cache
    return {
        "topic": n["topic"],
        "server": n["server"],
        "log": notify.load_notify_log(),
        "channel": _ntfy_channel_cache["v"],
    }


def ntfy_delete(msg_id: str) -> dict:
    """POST /api/ntfy/delete handler body. A successful remote delete also
    drops the matching local sent entry so the list mirrors the phone.
    Only entries for the currently configured topic are pruned, so a stale
    row from a previous topic can never be removed by a foreign-topic id."""
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", msg_id):
        raise ValueError("id must be 1-128 chars (letters, digits, - or _)")
    from . import notify
    cfg = coordinator.load_config()
    topic = notify._ntfy_cfg(cfg)["topic"]
    if not notify.delete_message(cfg, msg_id):
        raise RuntimeError("ntfy delete failed")
    # Reflect the delete right away instead of after the 10s channel TTL.
    _ntfy_channel_cache["v"] = [m for m in _ntfy_channel_cache["v"]
                                if m.get("seq", m.get("id")) != msg_id]
    notify.remove_log_entries(
        lambda e: (e.get("dir") == "sent"
                   and e.get("id") == msg_id
                   and e.get("topic") == topic))
    return {"ok": True, "deleted": True, "id": msg_id}


def ntfy_delete_all() -> dict:
    """POST /api/ntfy/delete-all handler body: send a DELETE tombstone for
    every message cached on the topic (enumerated from the server cache PLUS
    local-log ids; expired/unknown ids 404 -> treated as gone), then drop
    the matching local rows. Rows without an id (received replies) are
    cleared too; ids whose request failed keep their local row.
    NOTE (verified against ntfy.sh): a delete is a tombstone — it dismisses
    the notification on connected devices but the message STAYS in ntfy's
    pollable history until its cache TTL (12h) expires, so /api/ntfy/cache-
    ids will keep listing ids that were "deleted"."""
    from . import notify
    r = notify.delete_all(coordinator.load_config())
    # Everything goes except rows whose live message failed to delete (they
    # keep their delete button for a retry).
    failed = set(r.get("failed_ids") or [])
    notify.remove_log_entries(lambda e: e.get("id") not in failed)
    _ntfy_channel_cache["v"] = [m for m in _ntfy_channel_cache["v"]
                                if m.get("seq", m.get("id")) in failed]
    return {"ok": True, "deleted": r.get("deleted", 0),
            "total": r.get("total", 0)}


def ntfy_cache_ids() -> dict:
    """GET /api/ntfy/cache-ids: ids of the messages currently cached on the
    ntfy topic, from ntfy's pollable history (read-only). Each one gets a
    DELETE tombstone that dismisses the notification on connected devices.
    NB: ntfy's history is append-only — a tombstone does NOT remove the
    message from this enumeration; messages drop off by themselves when
    their cache TTL (12h on ntfy.sh) expires. Never treat this list as
    "what still needs deleting until empty" — it always lists everything
    published in the TTL window."""
    from . import notify
    cfg = coordinator.load_config()
    if not notify._ntfy_cfg(cfg)["topic"]:
        return {"ids": []}
    try:
        events = notify._ntfy_cached_events(cfg)
    except Exception as e:
        return {"error": str(e)}
    ids = []
    seen = set()
    for ev in events:
        i = str(ev.get("id") or "")
        if i and i not in seen:
            seen.add(i)
            ids.append(i)
    return {"ids": ids}


def ntfy_prune_local(payload: dict) -> dict:
    """POST /api/ntfy/prune-local: clear local log rows locally, no remote
    calls. Two modes:
      {"all": true}     -> remove every row. The clear-all flow uses this
        ONLY after the server cache has been verified empty, so the list
        never clears while messages still exist on the topic.
      {"ids": [...]}    -> remove rows whose id is listed (already gone)
        plus rows without an id. The server cache ids and the local log are
        DIFFERENT populations for a topic (cache = recent messages, log =
        the dashboard's own history), so cache membership must never be
        used to decide which local rows to drop."""
    from . import notify
    if payload.get("all") is True:
        return {"ok": True, "pruned": notify.remove_log_entries(lambda _e: True)}
    ids = payload.get("ids") or []
    if not isinstance(ids, list):
        raise ValueError("ids must be a list")
    idset = {str(i) for i in ids if isinstance(i, (str, int))}
    pruned = notify.remove_log_entries(
        lambda e: (e.get("id") in idset) or (not e.get("id")))
    return {"ok": True, "pruned": pruned}


def worker_idle_s(con, session_id: str) -> int:
    try:
        r = con.execute(
            "SELECT MAX(time_updated) FROM part WHERE session_id=?", (session_id,)).fetchone()
        if r and r[0]:
            return max(0, (utcms() - int(r[0])) // 1000)
    except Exception:
        pass
    return -1


def last_evaluated_summary(con, mid: str) -> str:
    """Human-readable subject of the last message the hawk evaluated.
    Resolves the message id via its text parts (prefers the first real content
    line, skips hawk-injected '[coordinator]' lines). Falls back to the id.
    llama.cpp sessions often carry their final message as 'reasoning' parts
    with no 'text' part, so reasoning text is used when no real text exists."""
    if not mid:
        return "—"
    try:
        rows = con.execute(
            "SELECT data FROM part WHERE message_id=?", (mid,)).fetchall()
    except Exception:
        return mid[:12] + "…"
    chunks = []
    reasoning_chunks = []
    for (data,) in rows:
        d = coordinator.parse_part_data(data)
        if not isinstance(d, dict):
            continue
        ptype = d.get("type")
        if ptype not in ("text", "assistant", "reasoning"):
            continue
        t = str(d.get("text", ""))
        if re.sub(r'^[\s"]+', "", t).startswith("[coordinator]"):
            continue
        if ptype == "reasoning":
            reasoning_chunks.append(t.strip())
        else:
            chunks.append(t.strip())
    if not chunks:
        chunks = reasoning_chunks
    body = "\n".join(c for c in chunks if c)
    for l in body.splitlines():
        ls = l.strip()
        if not ls or ls.startswith("#") or ls.startswith("[coordinator]") \
           or "STOP:" in ls or "RESULT:" in ls:
            continue
        return clean_title(ls, 90)
    return clean_title(body, 90) or (mid[:12] + "…")


# ── llama activity (read-only, reused from coordinator) ───────────────────────
_llama_probe_cache = {}  # {pid, base_wall, base_cpu, last_t, frac}
_CPU_CAP = float(os.cpu_count() or 1)  # cap cpu_frac at "all logical cores busy"


def _cpu_pct(frac: float) -> int:
    """cpu_frac ('cores busy', 1.0 = a full logical core) -> a 0-100 percentage
    relative to ALL logical cores, matching Task Manager's per-process view
    (100% = every core busy). Fixes the old display where 8 busy cores read as
    '800%'."""
    cap = max(1.0, float(os.cpu_count() or 1))
    return min(100, round(100.0 * max(0.0, frac) / cap))


def llama_status() -> dict:
    """Compact llama-server activity summary for the dashboard. Samples CPU
    time and computes a short-window delta against its own previous sample
    (non-blocking, no sleep). Returns idle on the very first sample."""
    now_s = time.time()
    c = _llama_probe_cache
    if now_s - c.get("last_t", 0) < 5:
        act = c.get("active", False)
        busy_s = 0
        if act and c.get("active_since_ms"):
            busy_s = max(0, (int(now_s * 1000) - c["active_since_ms"])) // 1000
        idle_s = 0
        if not act and c.get("idle_since_ms"):
            idle_s = max(0, (int(now_s * 1000) - c["idle_since_ms"])) // 1000
        return {"pid": c.get("pid"), "active": act,
                "cpu_frac": c.get("frac", 0.0),
                "cpu_pct": _cpu_pct(c.get("frac", 0.0)),
                "busy_for_s": busy_s, "idle_for_s": idle_s}
    out = {"pid": None, "active": False, "cpu_frac": 0.0, "cpu_pct": 0,
           "busy_for_s": 0, "idle_for_s": 0}
    try:
        cfg = coordinator.load_config()
        pid = coordinator.llama_pid(cfg)
        if pid:
            samp = coordinator._PROC_TIMES.sample(pid) if coordinator._PROC_TIMES \
                else None
            if samp is None:
                # lazily init the ctypes sampler
                if coordinator._PROC_TIMES is None:
                    coordinator._PROC_TIMES = coordinator.ProcessTimes()
                samp = coordinator._PROC_TIMES.sample(pid)
            if samp:
                wall_now, cpu_now = samp
                base_wall = c.get("base_wall")
                base_cpu = c.get("base_cpu")
                if base_wall is not None and wall_now > base_wall:
                    d_cpu = max(0, cpu_now - base_cpu)
                    d_wall = wall_now - base_wall
                    frac = max(0.0, min(d_cpu / d_wall if d_wall > 0 else 0.0, _CPU_CAP))
                else:
                    frac = 0.0
                # refresh baseline unconditionally
                c["base_wall"] = wall_now
                c["base_cpu"] = cpu_now
                out = {"pid": pid, "active": frac >= 0.02, "cpu_frac": round(frac, 2),
                        "cpu_pct": _cpu_pct(frac), "busy_for_s": 0, "idle_for_s": 0}
                if out["active"]:
                    if "active_since_ms" not in c:
                        c["active_since_ms"] = int(now_s * 1000)
                    c.pop("idle_since_ms", None)
                else:
                    if "idle_since_ms" not in c:
                        c["idle_since_ms"] = int(now_s * 1000)
                    c.pop("active_since_ms", None)
                busy_s = max(0, (int(now_s * 1000) - c.get("active_since_ms", 0))) // 1000 \
                    if c.get("active_since_ms") and out["active"] else 0
                idle_s = max(0, (int(now_s * 1000) - c.get("idle_since_ms", 0))) // 1000 \
                    if c.get("idle_since_ms") and not out["active"] else 0
                out["busy_for_s"] = busy_s
                out["idle_for_s"] = idle_s
                c["active"] = out["active"]
                c["frac"] = out["cpu_frac"]
                c["pid"] = pid
    except Exception:
        pass
    c["last_t"] = now_s
    return out


# ── HTTP ──────────────────────────────────────────────────────────────────────
def state_payload(override_sid: str | None = None) -> dict:
    cfg = coordinator.load_config()
    st = coordinator.load_state()
    selected = (override_sid or "").strip() or (cfg.get("session_id") or "").strip()
    pst = (st.get("per") or {}).get(selected) if selected else st
    if not isinstance(pst, dict):
        pst = st
    interval = int(cfg.get("poll_minutes", 8))
    last_poll = int(st.get("last_poll_at") or 0)
    now = utcms()
    offline = last_poll == 0 or (now - last_poll) // 1000 > interval * 60 * 3
    con = None
    idle = -1
    stitle = ""
    leval = ""
    leaf = None
    try:
        con = coordinator.connect_db(coordinator.db_path())
        idle = worker_idle_s(con, selected)
        stitle = session_title(con, selected)
        leval = last_evaluated_summary(con, pst.get("last_evaluated_mid") or "")
        recommended = recommended_session_id(con, cfg)
        if selected:
            row = coordinator.session_row(con, selected)
            leaf_node = coordinator.active_leaf(con, row) if row else None
            if leaf_node and leaf_node["id"] != selected:
                leaf = {"id": leaf_node["id"],
                        "title": leaf_node.get("title") or "",
                        "idle_s": worker_idle_s(con, leaf_node["id"])}
                # Root transcript is stale while a child has a live turn;
                # idle/mode should reflect the live leaf.
                idle = leaf["idle_s"]
    except Exception:
        recommended = ""
    finally:
        if con:
            con.close()
    proj = cfg.get("project_dir") or ""
    head = st.get("prev_head") or ""
    head_short = head[:8] if head else ""
    head_subject = ""
    branch = git_branch(proj) if proj else ""
    if head_short:
        for e in git_log_events(proj):
            if e["id"] == head_short:
                head_subject = short(e.get("detail", ""), 70)
                break
    llama = llama_status()
    if st.get("paused"):
        mode = "PAUSED"
    elif offline:
        mode = "MONITOR OFFLINE"
    elif 0 <= idle < 60 and llama.get("active"):
        mode = "BUSY"              # worker AND engine both active
    elif 0 <= idle < 60:
        mode = "BUSY (WORKER)"     # worker drove the session recently
    elif llama.get("active"):
        mode = "BUSY (LLAMA)"      # engine is decoding even though the worker
                                   # has been silent for a minute or more
    else:
        mode = "WATCHING"
    return {
        "mode": mode,
        "paused": bool(st.get("paused")),
        "monitor_online": not offline,
        "last_action": tail_last_log_line(),
        "next_poll_in_s": max(0, (last_poll + interval * 60000 - now) // 1000) if last_poll else -1,
        "poll_interval_s": interval * 60,
        "continues": int(pst.get("continues") or 0),
        "prev_head": head,
        "prev_head_short": head_short,
        "prev_head_subject": head_subject,
        "branch": branch,
        "last_evaluated_mid": pst.get("last_evaluated_mid") or "",
        "last_evaluated": leval,
        "session_id": selected,
        "session_title": stitle,
        "project_dir": cfg.get("project_dir") or "",
        "worker_idle_s": idle,
        "idle_minutes": int(cfg.get("idle_minutes") or 10),
        "selected": selected,
        "active_leaf": leaf,
        "recommended": recommended or "",
        "auto_watch": bool(recommended) and not (cfg.get("session_id") or "").strip(),
        "sessions": sessions_list_payload(),
        "llama": llama,
        "task_stream": _llama_stream_health(),
        "now_ms": now,
    }


class WebHandler(BaseHTTPRequestHandler):
    def _read_json_body(self):
        """Read a POST body, capped at MAX_BODY_BYTES. Returns (None, status)
        when the declared length is absent/invalid/oversized, else
        (parsed_object, 200). Non-JSON payloads raise ValueError."""
        try:
            length = int(self.headers.get("Content-Length", 0))
        except (TypeError, ValueError):
            return None, 400
        if length < 0:
            return None, 400
        if length > MAX_BODY_BYTES:
            return None, 413
        raw = self.rfile.read(length) if length else b""
        return (json.loads(raw.decode("utf-8")) if raw else {}), 200

    def _send_json(self, obj, status=200):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_404(self, msg="not found"):
        self._send_json({"error": msg}, 404)

    def _query(self):
        parts = self.path.split("?", 1)
        from urllib.parse import parse_qs
        return parse_qs(parts[1]) if len(parts) > 1 else {}

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        qs = self._query()
        if path == "/api/sessions":
            show_all = qs.get("all", ["0"])[0].lower() in ("1", "true", "yes")
            self._send_json(_cache_fetch(
                "sessions|%d" % show_all, SESSIONS_CACHE_S,
                lambda: sessions_list_payload(show_all)))
            return
        if path == "/api/session/stats":
            sid = qs.get("session_id", [""])[0]
            try:
                self._send_json(session_stats_payload(sid))
            except ValueError as e:
                self._send_json({"error": str(e)}, 400)
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/session/token-history":
            sid = qs.get("session_id", [""])[0]
            win_s = _window_seconds(qs.get("window", ["1h"])[0])
            try:
                all_sessions = (not sid or sid == "all")
                self._send_json(_cache_fetch(
                    "session_tokens|%s|%d" % (sid, win_s), MISC_CACHE_S,
                    lambda: (session_token_history_all(win_s) if all_sessions
                             else session_token_history(sid, win_s))))
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/":
            try:
                data = (WEB_DIR / "index.html").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(data)
            except FileNotFoundError:
                self._send_json({"error": "web/index.html missing"}, 500)
            return
        if path in ("/style.css", "/modern.css", "/app.js"):
            try:
                data = (WEB_DIR / path.lstrip("/")).read_bytes()
                ctype = "text/css; charset=utf-8" if path.endswith(".css") else \
                        "application/javascript; charset=utf-8"
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(data)
            except FileNotFoundError:
                self._send_404()
            return
        if path == "/modern.html":
            try:
                data = (WEB_DIR / path.lstrip("/")).read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(data)
            except FileNotFoundError:
                self._send_404()
            return
        if path == "/api/state":
            sid_q = qs.get("session", [None])[0] or ""
            self._send_json(_cache_fetch(
                "state|" + sid_q, STATE_CACHE_S,
                lambda: state_payload(sid_q or None)))
            return
        if path == "/api/timeline":
            try:
                try:
                    con = coordinator.connect_db(coordinator.db_path())
                except RuntimeError:
                    con = None  # no opencode DB yet: file-backed events only
                try:
                    cfg = coordinator.load_config()
                    raw = qs.get("session", [cfg.get("session_id") or ""])[0]
                    # "all" = aggregate every session (frontend default view).
                    sid = "" if raw in ("", "all") else raw
                    self._send_json(_cache_fetch(
                        "timeline|" + raw, TIMELINE_CACHE_S,
                        lambda: timeline_events(
                            cfg.get("project_dir") or "", sid, con,
                            since_ms=utcms() - TIMELINE_SCAN_WINDOW_S * 1000)))
                finally:
                    if con is not None:
                        con.close()
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/compactions":
            try:
                con = coordinator.connect_db(coordinator.db_path())
                try:
                    cfg = coordinator.load_config()
                    sid = qs.get("session", [cfg.get("session_id") or ""])[0]
                    self._send_json(_cache_fetch(
                        "compactions|" + sid, MISC_CACHE_S,
                        lambda: session_compaction_events(con, sid)))
                finally:
                    con.close()
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/escalations":
            sid_q = qs.get("session", [None])[0] or ""
            self._send_json(_cache_fetch(
                "escalations|" + sid_q, MISC_CACHE_S,
                lambda: escalations_list(sid_q or None)))
            return
        if path == "/api/llama":
            self._send_json(llama_telemetry_payload())
            return
        if path == "/api/llama/history":
            try:
                win_s = _window_seconds(qs.get("window", ["1h"])[0])
                sid = qs.get("session_id", [""])[0]
                self._send_json(_cache_fetch(
                    "history|%d|%s" % (win_s, sid), MISC_CACHE_S,
                    lambda: telemetry_history(win_s, session_id=sid)))
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/tasks":
            try:
                win_s = _window_seconds(qs.get("window", ["1h"])[0])
                self._send_json(_cache_fetch(
                    "tasks|%d" % win_s, TASKS_CACHE_S,
                    lambda: _tasks_payload(win_s)))
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/settings":
            self._send_json({
                "spec": {k: {"label": s["label"], "min": s.get("min"),
                             "max": s.get("max"), "type": s.get("type", "int"),
                             "step": s.get("step", 1)}
                         for k, s in SETTINGS_SPEC.items()},
                "values": settings_defaults(),
            })
            return
        if path == "/api/llama/restart/status":
            try:
                from . import llama_restart
                self._send_json(llama_restart.status(coordinator.load_config()))
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/notify/reply":
            self._queue_notify_reply(read_body=False)
            return
        if path == "/api/notify/kinds":
            try:
                from . import notify
                cfg = coordinator.load_config()
                self._send_json({
                    "all": list(notify.EVENT_KINDS),
                    "enabled": notify.kinds_enabled(cfg),
                    "defaults": list(notify.DEFAULT_NOTIFY_KINDS),
                })
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/notify/pending":
            try:
                from . import notify
                p = notify.load_pending_route()
                if p:
                    self._send_json({"pending": True, **p})
                else:
                    self._send_json({"pending": False})
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/ntfy":
            try:
                self._send_json(ntfy_payload())
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/ntfy/cache-ids":
            try:
                self._send_json(ntfy_cache_ids())
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        self._send_404()

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path == "/api/control":
            body, st = self._read_json_body()
            if st != 200:
                self._send_json({"error": "bad request"}, st)
                return
            try:
                intent = str(body.get("intent") or "")
                if intent not in ("pause", "resume", "poll_now"):
                    self._send_json({"error": "bad intent"}, 400)
                    return
                d = write_control(intent)
                self._send_json({"seq": d["seq"], "intent": d["intent"]})
            except Exception as e:
                self._send_json({"error": str(e)}, 400)
            return
        if path == "/api/settings":
            body, st = self._read_json_body()
            if st != 200:
                self._send_json({"error": "bad request"}, st)
                return
            try:
                if not isinstance(body, dict):
                    raise ValueError("body must be a JSON object")
                updated = update_settings(body)
                self._send_json({"ok": True, "updated": updated})
            except ValueError as e:
                self._send_json({"error": str(e)}, 400)
            except Exception as e:
                self._send_json({"error": str(e)}, 400)
            return
        if path == "/api/session":
            body, st = self._read_json_body()
            if st != 200:
                self._send_json({"error": "bad request"}, st)
                return
            try:
                result = update_session_settings(body)
                self._send_json({"ok": True, **result})
            except ValueError as e:
                self._send_json({"error": str(e)}, 400)
            except Exception as e:
                self._send_json({"error": str(e)}, 400)
            return
        if path == "/api/session/delete":
            body, st = self._read_json_body()
            if st != 200:
                self._send_json({"error": "bad request"}, st)
                return
            try:
                result = delete_session(body.get("session_id", ""))
                self._send_json({"ok": True, **result})
            except ValueError as e:
                self._send_json({"error": str(e)}, 400)
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/notify/reply":
            self._queue_notify_reply(read_body=True)
            return
        if path == "/api/notify/kinds":
            body, st = self._read_json_body()
            if st != 200:
                self._send_json({"error": "bad request"}, st)
                return
            try:
                from . import notify
                if not isinstance(body, dict) or not isinstance(body.get("enabled"), list):
                    raise ValueError("enabled must be a list of kind names")
                enabled = [k for k in body["enabled"] if k in notify.EVENT_KINDS]
                # config.json is merged per TOP-LEVEL key in load_config, so
                # the nested ntfy.kinds only survives if we rewrite the whole
                # notify key here.
                raw_path = coordinator.home_dir() / "config.json"
                try:
                    with open(raw_path, "r", encoding="utf-8-sig") as f:
                        raw = json.load(f)
                except Exception:
                    raw = {}
                if not isinstance(raw, dict):
                    raw = {}
                ntfy = dict((raw.get("notify") or {}).get("ntfy") or {})
                # Kinds not exposed as checkboxes (e.g. "test") are not in the
                # request body; keep them so the UI can't silently drop them.
                extras = [k for k in (ntfy.get("kinds") or [])
                          if isinstance(k, str) and k not in notify.EVENT_KINDS
                          and k not in enabled]
                ntfy["kinds"] = enabled + extras
                raw["notify"] = dict(raw.get("notify") or {})
                raw["notify"]["ntfy"] = ntfy
                coordinator.save_json(raw_path, raw)
                self._send_json({"ok": True, "enabled": enabled})
            except ValueError as e:
                self._send_json({"error": str(e)}, 400)
            except Exception as e:
                self._send_json({"error": str(e)}, 400)
            return
        if path == "/api/notify/route":
            body, st = self._read_json_body()
            if st != 200:
                self._send_json({"error": "bad request"}, st)
                return
            try:
                from . import notify
                idx = int(body.get("index") or 0)
                res = notify.resolve_pending_route(coordinator.load_config(), idx)
                self._send_json(res)
            except (ValueError, AttributeError) as e:
                self._send_json({"error": str(e)}, 400)
            except Exception as e:
                self._send_json({"error": str(e)}, 400)
            return
        if path == "/api/ntfy":
            body, st = self._read_json_body()
            if st != 200:
                self._send_json({"error": "bad request"}, st)
                return
            try:
                if not isinstance(body, dict):
                    raise ValueError("body must be a JSON object")
                topic = str(body.get("topic") or "").strip()
                if not topic:
                    raise ValueError("topic required")
                if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", topic):
                    raise ValueError("topic must be 1-64 chars (letters, digits, - or _)")
                cfg = coordinator.load_config()
                prev = cfg.setdefault("notify", {}).setdefault("ntfy", {})
                previous = str(prev.get("topic") or "")
                prev["topic"] = topic
                coordinator.save_json(coordinator.home_dir() / "config.json", cfg)
                self._send_json({"ok": True, "topic": topic, "previous": previous})
            except ValueError as e:
                self._send_json({"error": str(e)}, 400)
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/ntfy/test":
            try:
                from . import notify
                sent = notify.send_test(coordinator.load_config())
                self._send_json({"ok": True, "sent": sent})
            except Exception as e:
                self._send_json({"error": str(e)}, 400)
            return
        if path == "/api/ntfy/delete":
            body, st = self._read_json_body()
            if st != 200:
                self._send_json({"error": "bad request"}, st)
                return
            try:
                if not isinstance(body, dict):
                    raise ValueError("body must be a JSON object")
                self._send_json(ntfy_delete(str(body.get("id") or "")))
            except ValueError as e:
                self._send_json({"error": str(e)}, 400)
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/ntfy/delete-all":
            try:
                self._send_json(ntfy_delete_all())
            except ValueError as e:
                self._send_json({"error": str(e)}, 400)
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/ntfy/prune-local":
            body, st = self._read_json_body()
            if st != 200:
                self._send_json({"error": "bad request"}, st)
                return
            try:
                if not isinstance(body, dict):
                    raise ValueError("body must be a JSON object")
                self._send_json(ntfy_prune_local(body))
            except ValueError as e:
                self._send_json({"error": str(e)}, 400)
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/ntfy/backfill":
            # Recover full bodies for legacy sent entries from the ntfy cache.
            try:
                from . import notify
                self._send_json({"ok": True,
                                 **notify.backfill_bodies(coordinator.load_config())})
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if path == "/api/llama/restart":
            body, st = self._read_json_body()
            if st != 200:
                self._send_json({"error": "bad request"}, st)
                return
            try:
                if not isinstance(body, dict):
                    raise ValueError("body must be a JSON object")
                from . import llama_restart
                reason = str(body.get("reason") or "manual")[:80]
                dry = bool(body.get("dry_run"))
                if dry:
                    # Read-only: report what would happen, never touch the server.
                    self._send_json({"dry_run": True,
                                     **llama_restart.status(coordinator.load_config())})
                    return
                result = llama_restart.restart(coordinator.load_config(), reason)
                if result.get("ok"):
                    # Visibility: make sure the user knows this restart was
                    # intentional (plugin/health/time), not a crash.
                    try:
                        from . import notify
                        notify.send_text(coordinator.load_config(),
                                         "llama-server restarted (reason: %s).\n"
                                         "Old PID %s -> new PID %s in %.0fs; "
                                         "healthy: %s.\n"
                                         "This is by design, not a crash." % (
                                             reason, result.get("pid_old"),
                                             result.get("pid_new"),
                                             (result.get("elapsed_ms") or 0) / 1000.0,
                                             result.get("healthy")),
                                         kind="llama",
                                         eid="llamarestart:%d" % int(time.time()),
                                         meta={"reason": reason})
                    except Exception:
                        pass
                self._send_json(result)
            except ValueError as e:
                self._send_json({"error": str(e)}, 400)
            except Exception as e:
                self._send_json({"error": str(e)}, 400)
            return
        self._send_404()

    def log_message(self, *a):
        pass  # keep the dashboard console quiet

    def _queue_notify_reply(self, read_body: bool):
        import urllib.parse
        try:
            qs = urllib.parse.parse_qs(self.path.split("?", 1)[1]) if "?" in self.path else {}
        except Exception:
            qs = {}
        sid = (qs.get("session") or [""])[0]
        choice = (qs.get("choice") or [""])[0]
        if read_body:
            try:
                length = int(self.headers.get("Content-Length", 0))
            except (TypeError, ValueError):
                length = 0
            if length < 0 or length > MAX_BODY_BYTES:
                self._send_json({"error": "bad request"}, 413)
                return
            raw = self.rfile.read(length).decode("utf-8", "replace") if length else ""
            try:
                body = json.loads(raw) if raw else {}
                if isinstance(body, dict):
                    sid = sid or str(body.get("session") or "")
                    choice = choice or str(body.get("choice") or "")
            except Exception:
                form = urllib.parse.parse_qs(raw)
                sid = sid or (form.get("session") or [""])[0]
                choice = choice or (form.get("choice") or [""])[0]
        if not sid or not choice:
            self._send_json({"error": "session and choice required"}, 400)
            return
        try:
            from . import notify
            notify.queue_reply(sid, choice)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)
            return
        self._send_json({"queued": True, "session": sid[-8:],
                         "choice": choice[:40]})


# ── Response caching (single-flight TTL) ──────────────────────────────────
# The web UI polls /api/state + /api/timeline + /api/tasks every ~2 s; those
# build payloads from part-table scans over a multi-GB opencode.db. Cache each
# keyed payload for a short TTL and let concurrent misses collapse onto ONE
# computation, so a slow burst no longer stacks N full scans on the DB.

_RESP_CACHE: dict = {}
_RESP_CACHE_LOCK = threading.Lock()
_KEY_LOCKS: dict = {}

# TTLs (seconds). /api/state is polled every 2 s by the UI.
STATE_CACHE_S = 2.0
TIMELINE_CACHE_S = 5.0
TASKS_CACHE_S = 5.0
SESSIONS_CACHE_S = 5.0
MISC_CACHE_S = 5.0

# Timeline event scans only need recent history: the frontend window-filter
# shows a 200-item timeline capped at 24h/7d; scanning all time costs seconds
# on a multi-GB part table. 30 days covers every window the UI can pick.
TIMELINE_SCAN_WINDOW_S = 30 * 24 * 3600


def _tasks_payload(win_s: int) -> dict:
    """Full /api/tasks response: llama log registry + telemetry store merged,
    with owners assigned. Expensive (log parse + owner scans), so callers
    cache it (_cache_fetch). Raises on error (never cached)."""
    now_ms = utcms()
    cut = now_ms - win_s * 1000
    tasks = _llama_registry()
    anchors = {"pid": coordinator.llama_pid(coordinator.load_config()),
               "anchor_ms": _llama_anchor_ms()}
    con = _tele_db()
    try:
        _llama_assign_owners(con, now_ms)
        rows = con.execute(
            "SELECT task_id, start_wall, end_wall, tps, tokens, "
            "owner, kind, status FROM llama_tasks "
            "WHERE start_wall >= ? ORDER BY start_wall DESC",
            (cut,)).fetchall()
        persisted = {int(r[0]): {"task_id": int(r[0]),
                                 "start_wall": int(r[1]),
                                 "end_wall": int(r[2]),
                                 "tps": float(r[3] or 0),
                                 "tokens": int(r[4] or 0),
                                 "owner": r[5] or "",
                                 "kind": r[6] or "",
                                 "status": r[7] or ""}
                     for r in rows}
    finally:
        con.close()
    merged = persisted
    for tid, rec in tasks.items():
        if rec.get("start", 0) >= cut:
            merged[int(tid)] = {"task_id": int(tid),
                                "start_wall": rec.get("start") or 0,
                                "end_wall": rec.get("end") or 0,
                                "tps": rec.get("tps") or 0.0,
                                "tokens": rec.get("tokens") or 0,
                                "owner": rec.get("owner") or "",
                                "kind": rec.get("kind") or "",
                                "status": rec.get("status") or ""}
    return {
        "ok": True,
        "window": "now" if win_s == 0 else "now-%ds" % win_s,
        "llama": anchors,
        "stream": _llama_stream_health(),
        "tasks": sorted(merged.values(),
                        key=lambda t: -t["start_wall"])[:500]}


def _cache_prune(now: float) -> None:
    if len(_RESP_CACHE) <= 32:
        return
    for k in [k for k, (exp, _) in _RESP_CACHE.items() if exp <= now]:
        _RESP_CACHE.pop(k, None)
        _KEY_LOCKS.pop(k, None)


def _cache_fetch(key: str, ttl_s: float, builder):
    """Return a cached payload computed at most once per TTL per key. A slow
    builder blocks only the first caller for its key (single-flight); other
    keys proceed on their own locks. Builder errors propagate (never cached)."""
    now = time.time()
    hit = _RESP_CACHE.get(key)
    if hit and hit[0] > now:
        return hit[1]
    with _RESP_CACHE_LOCK:
        lock = _KEY_LOCKS.setdefault(key, threading.Lock())
    with lock:
        hit = _RESP_CACHE.get(key)
        if hit and hit[0] > now:
            return hit[1]
        v = builder()
        with _RESP_CACHE_LOCK:
            _cache_prune(time.time())
            _RESP_CACHE[key] = (time.time() + ttl_s, v)
        return v


class _ExclusiveHTTPServer(ThreadingHTTPServer):
    """Windows: SO_EXCLUSIVEADDRUSE so no second dashboard.py instance can
    share the port. With the default SO_REUSEADDR two processes CAN both
    bind (Windows semantics) — a stale instance then keeps serving OLD code
    while a freshly launched one also runs, which is how the ntfy clear-all
    ran the pre-fix behaviour after a 'restart'. A duplicate now fails to
    bind and exits, so the newest process always wins."""
    allow_reuse_address = False

    def server_bind(self):
        if os.name == "nt":
            try:
                self.socket.setsockopt(
                    socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            except OSError:
                pass
        super().server_bind()


def make_server(port=0, bind="127.0.0.1"):
    return _ExclusiveHTTPServer((bind, port), WebHandler)


def serve(port=DEFAULT_PORT, open_browser=False, bind="127.0.0.1") -> int:
    # The timeline event scans filter/order by part.time_created; without an
    # index SQLite full-scans the whole part table (~90k rows, multi-GB) and
    # sorts with a temp b-tree per request. Create the index once (benign for
    # opencode; survives restarts). connect_db() is read-only, so use a
    # dedicated writable connection. Failure degrades gracefully.
    try:
        con = sqlite3.connect(str(coordinator.db_path()), timeout=30)
        try:
            con.execute("PRAGMA busy_timeout=30000")
            con.execute("CREATE INDEX IF NOT EXISTS idx_part_time_created "
                        "ON part(time_created)")
            # Expression index so the subagent/owner scans hit only the 600
            # task-tool rows instead of JSON-decoding all 90k parts per
            # request (2026-10-02: each scan cost ~800 ms, which made every
            # session switch and 2 s UI poll rebuild take seconds).
            con.execute("CREATE INDEX IF NOT EXISTS idx_part_type_tool ON "
                        "part(json_extract(data, '$.type'), "
                        "json_extract(data, '$.tool'))")
            con.commit()
        finally:
            con.close()
    except Exception as e:
        coordinator.log_debug("dashboard: part(time_created) index: %s" % e)
    threading.Thread(target=_tele_loop, daemon=True).start()
    try:
        if coordinator.load_config().get("llama_task_tracking", True):
            _llama_registry_restore((coordinator.load_state() or {}).get("llama_tasks"))
            threading.Thread(target=_llama_tail_loop, daemon=True).start()
    except Exception as e:
        coordinator.log_debug("llama_task_tracking init: %s" % e)
    srv = make_server(port, bind)
    print("dashboard on http://%s:%d" % (bind, srv.server_address[1]), flush=True)
    if open_browser:
        threading.Thread(target=lambda: subprocess.Popen(["cmd", "/c", "start", "",
                          "http://%s:%d" % ("127.0.0.1", srv.server_address[1])]), daemon=True).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        return 0
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="hawk coordinator dashboard")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--listen", default="127.0.0.1",
                    help="bind address (use 0.0.0.0 so a phone can reach the "
                         "notify reply endpoint on your LAN)")
    ap.add_argument("--open", action="store_true", help="open the default browser")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args(argv)
    if args.self_test:
        return self_test()
    new_topic = coordinator.ensure_ntfy_topic(coordinator.load_config())
    if new_topic:
        print("ntfy: generated topic %s for this install" % new_topic)
    return serve(args.port, args.open, args.listen)


def self_test():
    """Run the scenarios in tests/selftest_dashboard.py (source checkout only)."""
    tests = Path(__file__).resolve().parent.parent / "tests"
    if not (tests / "selftest_dashboard.py").exists():
        print("self-test: tests/ not found (they ship with the source checkout, "
              "not the installed package)")
        return 1
    sys.path.insert(0, str(tests))
    import selftest_dashboard
    return selftest_dashboard.self_test()


# Moved to telemetry.py; bound here so `dashboard.<name>` keeps working (one shared namespace).
from .telemetry import (  # noqa: E402
    HW_TTL_S,
    MAX_SERIES,
    MIN_DISK_BYTES,
    SUBAGENT_PUSH_LOOKBACK_MS,
    TELE_DB_NAME,
    TELE_DEFAULT_WINDOW_S,
    TELE_INTERVAL_S,
    TELE_RETENTION_S,
    TELE_TARGET_POINTS,
    TELE_WINDOWS,
    _HW,
    _HW_LOCK,
    _POINT_KEYS,
    _VRAM_CACHE,
    _compaction_detected,
    _db_total_tokens_cache,
    _downsample,
    _event_push_sample,
    _hw_disk_inventory_sync,
    _hw_gpu_totals_sync,
    _hw_sticks,
    _hw_sticks_sync,
    _ptps,
    _purge_old,
    _row_to_point,
    _slots_decoded,
    _tele_backfill_totals,
    _tele_compactions,
    _tele_db,
    _tele_db_path,
    _tele_last_raw,
    _tele_lock,
    _tele_loop,
    _tele_meta,
    _tele_purge_at,
    _tele_restore_state,
    _tele_sample,
    _tele_series,
    _tps,
    _window_seconds,
    gpu_system_max,
    hw_disk_activity,
    hw_gpus,
    hw_ram_sticks,
    hw_ram_sticks_info,
    llama_telemetry_payload,
    llama_vram_mb,
    telemetry_history,
    telemetry_record,
)


# Moved to llama_tasks.py; bound here so `dashboard.<name>` keeps working (one shared namespace).
from .llama_tasks import (  # noqa: E402
    LLAMA_FUTURE_SLACK_MS,
    LLAMA_GRACE_MS,
    LLAMA_LOG_FN,
    LLAMA_ORPHAN_MS,
    LLAMA_TAIL_INTERVAL_S,
    LLAMA_TASKS_LIMIT,
    UNCAPTURED_MARGIN_S,
    _LLAMA_ANCHOR_MS,
    _LLAMA_ANCHOR_POS,
    _LLAMA_CAP,
    _LLAMA_LINE_RE,
    _LLAMA_TAIL_POS,
    _LLAMA_TASKS,
    _LLAMA_TASKS_LOCK,
    _TICK_RE,
    _llama_anchor_ms,
    _llama_apply,
    _llama_assign_owners,
    _llama_boot_start,
    _llama_capture_cause,
    _llama_child_last_activity,
    _llama_child_terminal,
    _llama_is_future,
    _llama_owned_tasks,
    _llama_owner_state,
    _llama_owner_windows,
    _llama_record,
    _llama_registry,
    _llama_registry_restore,
    _llama_save_persisted,
    _llama_stream_cause,
    _llama_stream_health,
    _llama_tail_loop,
    _llama_tasks_table,
    _llama_tick_ms,
    _tick_to_ms,
    parse_llama_line,
)


if __name__ == "__main__":
    sys.exit(main())
