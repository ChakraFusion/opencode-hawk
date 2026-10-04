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


def iso(ms: int) -> str:
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


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


def stop_title(body: str, kind: str = "VERIFIED") -> str:
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
    hawk escalation summaries that quote 'STOP: VERIFIED', decision write-ups)
    used to leak in as false milestones because the marker was searched
    anywhere in the body; it is now anchored to the last line so those are
    dropped.

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
        r"^STOP:\s*(VERIFIED|NEEDS_DECISION|BLOCKED)(?:\s+\S+)*$|"
        r"STOP:\s*(VERIFIED|NEEDS_DECISION|BLOCKED)\s*$")
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
                    "detail": body.strip()[:220], "ok": kind == "VERIFIED", "tests": tests})
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


# ── llama task activity engine (design 2026-09-23) ────────────────────────────
# Provider-truth activity: capture llama-server's console (the log file every
# dashboard-managed spawn appends), turn task ids into a wall-clock registry.
# opencode.db is used ONLY for structure (owner windows, Task 2).
#
# Log format (llama-server task log):
#   NNN.NN.NNN.NNN I slot launch_slot_: id  0 | task 18    | processing task, ...
#   NNN.NN.NNN.NNN I slot print_timing: id  0 | task 152   | n_gen =    282, tg =  11.35 t/s, ...
#   NNN.NN.NNN.NNN I slot      release: id  0 | task 64623 | stop processing: n_tokens = 41452, ...
# Words/numbers are whitespace-padded, hence flexible \s+/\s* everywhere.

LLAMA_LOG_FN = "llama-server.out.log"
LLAMA_GRACE_MS = 5000        # tool-loop grace: 5 s with no new task => finished
# A task part opencode still calls running is only declared finished after
# this long without any child activity (interrupted-shutdown orphans).
LLAMA_ORPHAN_MS = 30 * 60 * 1000
LLAMA_TAIL_INTERVAL_S = 1.0
LLAMA_TASKS_LIMIT = 2000     # state.json registry bound (newest tasks kept)

_LLAMA_TASKS: dict[int, dict] = {}
_LLAMA_TASKS_LOCK = threading.Lock()
_LLAMA_TAIL_POS = 0
_LLAMA_ANCHOR_MS = 0
_LLAMA_ANCHOR_POS = 0    # read-start offset: first byte of the current
                         # anchor's era that may need a re-parse on respawn

_LLAMA_LINE_RE = re.compile(
    r"^(\d+)\.(\d+)\.(\d+)\.(\d+) I slot\s+"
    r"(?P<kind>launch_slot_|release|print_timing): "
    r"id\s+(?P<slot>\d+) \| task\s+(?P<task>\d+) \| (?P<rest>.*)$")

_TICK_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:\.(\d+))?\s")


def _tick_to_ms(a, b, c, d) -> int:
    """Log prefix A.B.C.D -> ms since server start. Unit pinned by the Task-0
    spike: MM:SS.mmm with D sub-ms, so tick = (a*60 + b)*1000 + c. This is the
    single place the unit lives."""
    return (int(a) * 60 + int(b)) * 1000 + int(c)


def parse_llama_line(line: str):
    """One console line -> {tick, kind, task, slot, ...} | None."""
    m = _LLAMA_LINE_RE.match(line.rstrip("\r\n"))
    if not m:
        return None
    ev = {"tick": _tick_to_ms(*[m.group(i) for i in range(1, 5)]),
          "kind": m.group("kind"), "task": int(m.group("task")),
          "slot": int(m.group("slot"))}
    rest = m.group("rest")
    if ev["kind"] == "release":
        mt = re.search(r"n_tokens\s*=\s*(\d+),\s*truncated\s*=\s*(\d+)", rest)
        ev["tokens"] = int(mt.group(1)) if mt else 0
        ev["truncated"] = int(mt.group(2)) if mt else 0
    elif ev["kind"] == "print_timing":
        mt = re.search(r"tg\s*=\s*([\d.]+)\s*t/s", rest)
        ev["tps"] = float(mt.group(1)) if mt else 0.0
        mn = re.search(r"n_gen\s*=\s*(\d+)", rest)
        ev["n_gen"] = int(mn.group(1)) if mn else 0
    return ev


def _llama_registry() -> dict:
    """Live snapshot for rendering / persistence / tests."""
    with _LLAMA_TASKS_LOCK:
        return {k: dict(v) for k, v in _LLAMA_TASKS.items()}


def _llama_registry_restore(data) -> None:
    """Replace the registry from state.json (restart survival)."""
    with _LLAMA_TASKS_LOCK:
        _LLAMA_TASKS.clear()
        for k, v in (data or {}).items():
            try:
                if (_llama_is_future(int(v.get("start") or 0))
                        or _llama_is_future(int(v.get("end") or 0))):
                    continue          # stale future-dated entry: drop
                _LLAMA_TASKS[int(k)] = {
                    "start": int(v.get("start") or 0),
                    "end": int(v.get("end") or 0),
                    "tps": float(v.get("tps") or 0.0),
                    "tokens": int(v.get("tokens") or 0),
                    "owner": str(v.get("owner") or ""),
                    "kind": str(v.get("kind") or ""),
                    "status": str(v.get("status") or "orphan")}
            except (TypeError, ValueError, AttributeError):
                continue


def _llama_owned_tasks(owner: str) -> list:
    """Registry tasks whose owner key matches (empty owner string matches
    nothing)."""
    with _LLAMA_TASKS_LOCK:
        return [dict(v) for v in _LLAMA_TASKS.values()
                if v.get("owner") == owner]


def _llama_anchor_ms() -> int:
    """Wall clock of the log's uptime zero: the spawn time of the current
    llama-server (restart record), else its process start time, else 0."""
    try:
        from . import llama_restart
        st = llama_restart.load_state()
        if st.get("last_at_ms"):
            return int(st["last_at_ms"])
        pid = coordinator.llama_pid(coordinator.load_config())
        spec = llama_restart._process_spec(pid) if pid else None
        if spec and spec.get("started_ms"):
            return int(spec["started_ms"])
    except Exception:
        pass
    return 0


# A task wall later than now + this slack cannot be real: it means a line
# from an older server process was dated under the current anchor. Such
# events are dropped (never stored), and stale rows are purged on open.
LLAMA_FUTURE_SLACK_MS = 120_000


def _llama_is_future(wall: int, now_ms: int = 0) -> bool:
    return bool(wall) and wall > (now_ms or utcms()) + LLAMA_FUTURE_SLACK_MS


def _llama_apply(ev: dict, anchor: int) -> None:
    """Fold one parsed log event into the registry (idempotent, thread-safe).
    A release for an unseen task id (log resumed past its launch line) lands
    as an orphan: start = release wall, status released (fallback 4)."""
    wall = anchor + ev["tick"] if anchor else 0
    if _llama_is_future(wall):
        coordinator.log_debug("llama_task %s dropped: future wall %s"
                              % (ev.get("task"), wall))
        return
    tid = ev["task"]
    with _LLAMA_TASKS_LOCK:
        rec = _LLAMA_TASKS.get(tid)
        if rec is None:
            rec = {"start": 0, "end": 0, "tps": 0.0, "tokens": 0,
                   "owner": "", "kind": "", "status": "orphan"}
            _LLAMA_TASKS[tid] = rec
        if ev["kind"] == "launch_slot_":
            rec.update({"start": wall, "tps": 0.0, "tokens": 0,
                        "status": "running"})
        elif ev["kind"] == "release":
            if not rec.get("start"):
                rec["start"] = wall
            rec.update({"end": wall, "tokens": ev.get("tokens") or 0,
                        "status": "released"})
        elif ev["kind"] == "print_timing" and ev.get("tps"):
            rec["tps"] = round(ev["tps"], 2)


def _llama_tasks_table(con) -> None:
    """llama_tasks DDL (hawk_telemetry.db). Idempotent."""
    con.execute(
        "CREATE TABLE IF NOT EXISTS llama_tasks ("
        " task_id INTEGER, start_wall INTEGER, end_wall INTEGER, "
        " tps REAL, tokens INTEGER, owner TEXT, kind TEXT, status TEXT, "
        " PRIMARY KEY (task_id, start_wall))")
    cutoff = utcms() + LLAMA_FUTURE_SLACK_MS
    con.execute("DELETE FROM llama_tasks WHERE start_wall > ? OR end_wall > ?",
                (cutoff, cutoff))
    con.commit()


def _llama_record(tid: int, con) -> None:
    """Persist one task row (best-effort; called on release from the tailer
    and from the /api/tasks read path)."""
    try:
        with _LLAMA_TASKS_LOCK:
            rec = dict(_LLAMA_TASKS.get(tid) or {})
        if not rec.get("end"):
            return
        con.execute(
            "INSERT OR REPLACE INTO llama_tasks "
            "(task_id,start_wall,end_wall,tps,tokens,owner,kind,status) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (tid, rec.get("start") or 0, rec.get("end") or 0,
             rec.get("tps") or 0.0, rec.get("tokens") or 0,
             rec.get("owner") or "", rec.get("kind") or "",
             rec.get("status") or "released"))
        con.commit()
    except Exception as e:
        coordinator.log_debug("llama_task_record: %s" % e)


def _llama_save_persisted() -> None:
    """state.json keeps the registry (restart survival), bounded to the
    newest LLAMA_TASKS_LIMIT tasks plus anything still in flight."""
    try:
        st = coordinator.load_state()
        snap = sorted(_llama_registry().items(),
                      key=lambda kv: kv[1].get("start") or 0)
        keep = [kv for kv in snap if kv[1].get("status") != "released"]
        keep += snap[-LLAMA_TASKS_LIMIT:] if len(snap) > LLAMA_TASKS_LIMIT else []
        st["llama_tasks"] = dict(keep)
        coordinator.save_state(st)
    except Exception as e:
        coordinator.log_debug("llama_tasks save: %s" % e)


def _llama_stream_cause(pid, anchor, busy, last_rel_ms, window_s, now_ms) -> str:
    """Degradation causes in priority order: no-server, no-anchor, gap."""
    if not pid:
        return "no-server"
    if not anchor:
        return "no-anchor"
    if busy and last_rel_ms and now_ms - last_rel_ms > window_s * 1000:
        return "gap"
    return ""


def _llama_stream_health() -> dict:
    """task-stream status line (design 4.6): {'ok': bool, 'cause': str,
    'hint'?: str} - hint only for the 'uncaptured' triage."""
    cfg = coordinator.load_config()
    pid = coordinator.llama_pid(cfg)
    anchor = _llama_anchor_ms()
    now_ms = utcms()
    with _LLAMA_TASKS_LOCK:
        last_rel = max([rec.get("end") or 0
                        for rec in _LLAMA_TASKS.values()] or [0])
    busy = bool((llama_status() or {}).get("active"))
    cause = _llama_stream_cause(pid, anchor, busy, last_rel,
                                int(cfg.get("stall_llama_window_s", 360)),
                                now_ms)
    out = {"ok": not cause, "cause": cause or "ok"}
    if not cause and pid:
        # Capture lapse = llama BUSY while the console log stops growing.
        # Idle llama writes nothing, so silence alone is healthy. Growth is
        # judged by file SIZE: on Windows the mtime of a file held open for
        # append by llama-server is updated lazily and cannot be trusted.
        try:
            log_stat = (coordinator.home_dir() / LLAMA_LOG_FN).stat()
            size = log_stat.st_size
            mtime = log_stat.st_mtime
        except OSError:
            size = -1
            mtime = 0.0
        server_start_s = 0.0
        try:
            from . import llama_restart
            spec = llama_restart._process_spec(pid)
            if spec and spec.get("started_ms"):
                server_start_s = spec["started_ms"] / 1000.0
        except Exception:
            pass
        cause = _llama_capture_cause(size, busy, time.time(),
                                     server_start_s=server_start_s,
                                     log_mtime=mtime)
        out["cause"] = cause or "ok"
        out["ok"] = not cause
        if cause == "uncaptured":
            # The llama IS healthy (busy/active) -- only its console output was
            # never captured (spawned outside hawk). That is not a health
            # degradation, so report ok; the hint explains the capture lapse.
            out["ok"] = True
            out["hint"] = ("llama is healthy but its output is not captured "
                           "(spawned outside hawk); the log is stale, not broken")
    return out


_LLAMA_CAP = {"size": -1, "since": 0.0, "grew_at": 0.0}

# A server whose log's last observed growth predates its own spawn by more
# than this margin is running UNcaptured (stdout went to DEVNULL or a raw
# cmd), so "frozen log" is a false alarm, not a capture lapse.
UNCAPTURED_MARGIN_S = 60.0


def _llama_capture_cause(size: int, busy: bool, now_s: float,
                         grace_s: float = 0.0,
                         server_start_s: float = 0.0,
                         log_mtime: float = 0.0) -> str:
    """Capture-lapse triage for the task stream.
      ''           - healthy: log is growing, or llama is idle (a silent
                     server legitimately writes nothing)
      'no-capture' - log file missing, or llama busy while the log size
                     stayed frozen past the grace window AND the current
                     server plausibly owns the log
      'uncaptured' - the log's last observed growth predates the current
                     server's spawn: this server's stdout was never captured
                     (spawned outside hawk, or by the old DEVNULL respawn
                     path), so 'degraded' would be a false alarm. The caller
                     attaches a hint instead.
    """
    if size < 0:
        return "no-capture"
    grace = grace_s or 4 * LLAMA_TAIL_INTERVAL_S
    if size != _LLAMA_CAP["size"] or not busy:
        if size != _LLAMA_CAP["size"] and _LLAMA_CAP["size"] < 0:
            # First sight after a loss: can't date growth, seed from the file
            # mtime (lazy/coarse on Windows, but the margin is wide and a
            # stale seed only biases toward the honest 'uncaptured' label).
            if log_mtime > 0:
                _LLAMA_CAP["grew_at"] = log_mtime
        elif size != _LLAMA_CAP["size"]:
            _LLAMA_CAP["grew_at"] = now_s
        _LLAMA_CAP["size"] = size
        _LLAMA_CAP["since"] = now_s
        return ""
    if now_s - _LLAMA_CAP["since"] <= grace:
        return ""
    if server_start_s and _LLAMA_CAP["grew_at"] and \
            _LLAMA_CAP["grew_at"] + UNCAPTURED_MARGIN_S < server_start_s:
        return "uncaptured"
    return "no-capture"


def _llama_tick_ms(line: str):
    """Leading A.B.C(.D) prefix of ANY console line -> ms since boot start
    (same unit as _tick_to_ms), or None for lines without the prefix."""
    m = _TICK_RE.match(line)
    if not m:
        return None
    return (int(m.group(1)) * 60 + int(m.group(2))) * 1000 + int(m.group(3))


def _llama_boot_start(log) -> int:
    """Byte offset of the NEWEST boot's first line: a tick smaller than the
    previous line's marks an uptime restart (server spawn). Bytes before it
    belong to older processes whose tick is NOT wall-clock-equivalent under
    the current anchor, so the tailer begins there. 0 when no restart is
    found (single boot / file already begins mid-boot -> read everything).

    Scans in BINARY so offsets are exact byte positions: the console log is
    CRLF on Windows, and text-mode line lengths would undercount one byte
    per line, landing the boundary inside the previous era."""
    try:
        if not log.exists():
            return 0
        size = log.stat().st_size
        tail = min(size, 262144)
        with open(log, "rb") as f:
            f.seek(size - tail)
            raw = f.read(tail)
    except OSError:
        return 0
    base = size - tail
    boundary = 0
    prev = None
    off = 0
    for chunk in raw.split(b"\n"):
        t = _llama_tick_ms(chunk.decode("utf-8", "replace"))
        if t is not None:
            if prev is not None and t < prev:
                boundary = base + off    # this line opens a new boot
            prev = t
        off += len(chunk) + 1            # +1 for the split "\n" (CR kept)
    return boundary


def _llama_tail_loop() -> None:
    """Tail llama-server.out.log for the dashboard's lifetime: parse new
    lines, fold into the registry, assign owners (Task 2), persist releases
    to hawk_telemetry.db + state.json.

    Era discipline: the log is append-only across respawns ("ab" in
    llama_restart) and can concatenate several server boots. Every line's
    tick is uptime since ITS process's start, so one anchor only fits lines
    of the CURRENT era:
      * first establishment - begin at the newest boot boundary, ignoring
        older processes' lines (a fresh dashboard must not date stale
        history with the current spawn time - that would mint future walls).
      * respawn (anchor change) - rewind to the newest boot boundary
        (fallback: _LLAMA_ANCHOR_POS) and re-parse that era: llama_restart
        appends boot lines BEFORE it commits last_at_ms, so a plain tail
        would strand them under the old anchor.
        A re-parse is idempotent - the registry is keyed by task id and
        llama_tasks rows by (task_id, start_wall).
      * shrink (truncation/rotation) - restart at 0 under the same anchor.
    """
    global _LLAMA_TAIL_POS, _LLAMA_ANCHOR_MS, _LLAMA_ANCHOR_POS
    log = coordinator.home_dir() / LLAMA_LOG_FN
    while True:
        try:
            anchor = _llama_anchor_ms()
            if anchor:
                if _LLAMA_ANCHOR_MS == 0:
                    _LLAMA_ANCHOR_MS = anchor
                    _LLAMA_TAIL_POS = _llama_boot_start(log)
                    _LLAMA_ANCHOR_POS = _LLAMA_TAIL_POS
                elif anchor != _LLAMA_ANCHOR_MS:
                    _LLAMA_ANCHOR_MS = anchor
                    # Rewind to the NEWEST boot boundary, not merely to the
                    # start of the last read: that read can also hold the
                    # previous process's final lines, whose large uptime
                    # ticks would be dated under the new anchor -> future
                    # walls. Fall back to the last read start if no boundary
                    # is visible (single boot in the file).
                    boot = _llama_boot_start(log)
                    _LLAMA_TAIL_POS = boot if boot else _LLAMA_ANCHOR_POS
            try:
                size = log.stat().st_size if log.exists() else 0
            except OSError:
                size = 0
            if size < _LLAMA_TAIL_POS:
                _LLAMA_TAIL_POS = 0        # file truncated/replaced
                _LLAMA_ANCHOR_POS = 0
            if size > _LLAMA_TAIL_POS and _LLAMA_ANCHOR_MS:
                _LLAMA_ANCHOR_POS = _LLAMA_TAIL_POS   # era starts at this read
                with open(log, "r", encoding="utf-8", errors="replace") as f:
                    f.seek(_LLAMA_TAIL_POS)
                    fresh = f.read(size - _LLAMA_TAIL_POS).splitlines()
                _LLAMA_TAIL_POS = size
                released = []
                for ln in fresh:
                    ev = parse_llama_line(ln)
                    if ev:
                        _llama_apply(ev, _LLAMA_ANCHOR_MS)
                        if ev["kind"] == "release":
                            released.append(ev["task"])
                if released:
                    # Owners come from opencode.db structure; llama_tasks
                    # history lives in hawk_telemetry.db (_tele_db), kept in
                    # sync here - the table's only write path.
                    con = None
                    try:
                        con = coordinator.connect_db(coordinator.db_path())
                        _llama_assign_owners(con, utcms())
                    except Exception as e:
                        # owner correlation is best-effort: a locked/absent
                        # opencode.db must never block persisting releases
                        coordinator.log_debug("llama_owner_assign: %s" % e)
                    finally:
                        if con is not None:
                            con.close()
                    tcon = _tele_db()
                    try:
                        for tid in released:
                            _llama_record(tid, tcon)
                    finally:
                        tcon.close()
                    _llama_save_persisted()
        except Exception as e:
            coordinator.log_debug("llama_tailer: %s" % e)
        time.sleep(LLAMA_TAIL_INTERVAL_S)


def _llama_owner_windows(con, now_ms: int) -> list:
    """Owner windows from opencode structure: task-tool part windows
    (kind 'subagent', deepest) + llama-served session lifetimes (kind
    'session'). Windows are half-open [start, end], end==0 => now."""
    wins = []
    try:
        rows = con.execute(
            "SELECT id, data FROM part WHERE "
            "json_extract(data, '$.type')='tool' AND "
            "json_extract(data, '$.tool')='task'").fetchall()
    except Exception:
        rows = []
    for pid, d in rows:
        try:
            pd = json.loads(d)
            t = ((pd.get("state") or {}).get("time") or {})
            start = int(t.get("start") or 0)
            if not start:
                continue
            end = int(t.get("end") or 0)
            if not end:
                # Structurally open part (opencode never wrote a terminal
                # wall - e.g. an interrupted shutdown). Cap the window at
                # INDEPENDENT terminal evidence (the child session's last
                # activity) instead of 'now', so a zombie part cannot
                # absorb every subsequent llama task on the box. An
                # in-flight task owned by the part only keeps the window
                # open while it started BEFORE the cap: a task started
                # after the child froze is a misattribution, not life.
                meta = ((pd.get("state") or {}).get("metadata") or {})
                child = str(meta.get("sessionId") or "")
                if not child:
                    m = re.search(r'id="([^"]+)"',
                                  str((pd.get("state") or {}).get("output") or ""))
                    child = m.group(1) if m else ""
                cap = _llama_child_last_activity(con, child) if child else 0
                if cap:
                    live = [r for r in _llama_owned_tasks("part:" + pid)
                            if not r.get("end") and (r.get("start") or 0) <= cap]
                    end = now_ms if live else cap
                else:
                    end = now_ms
        except Exception:
            continue
        wins.append({"key": "part:" + pid, "start": start, "end": end,
                     "kind": "subagent"})
    try:
        srows = con.execute(
            "SELECT id, time_created, time_updated, model FROM session").fetchall()
    except Exception:
        srows = []
    for sid, tc, tu, model in srows:
        try:
            prov = (json.loads(model or "{}") or {}).get("providerID", "")
        except Exception:
            prov = ""
        if prov != "llama.cpp":
            continue
        wins.append({"key": "session:" + sid, "start": int(tc or 0),
                     "end": int(tu or 0) or now_ms, "kind": "session"})
    return wins


def _llama_assign_owners(con, now_ms: int) -> None:
    """Serial-slot correlation: a task launched at wall T belongs to the
    deepest active owner window containing T (kind subagent beats session;
    ties -> latest window start). Fills tasks lacking an owner; idempotent -
    an assigned owner is never overwritten."""
    wins = _llama_owner_windows(con, now_ms)
    if not wins:
        return
    with _LLAMA_TASKS_LOCK:
        for tid, rec in _LLAMA_TASKS.items():
            if rec.get("owner") or not rec.get("start"):
                continue
            t0 = rec["start"]
            cand = [w for w in wins if w["start"] <= t0 <= w["end"]]
            if not cand:
                continue
            w = max(cand, key=lambda w: (w["kind"] == "subagent", w["start"]))
            rec["owner"] = w["key"]
            rec["kind"] = w["kind"]


def _llama_child_last_activity(con, child: str) -> int:
    """Latest wall the child session demonstrably wrote anything: last
    assistant-message completion, any message row (created/updated), any
    part row (created/updated). 0 when the child is unknown. This is
    INDEPENDENT terminal evidence: a task-tool part whose opencode
    `time.end` was never written (interrupted shutdown) can be capped at
    this wall instead of 'now', so the orphaned part stops absorbing every
    llama task on the box (2026-10-01: a Sep-28 qa part did exactly that
    for 3 days, minting a 4528.7 min 'finished' ring from a bogus end wall)."""
    if not child:
        return 0
    try:
        r = con.execute(
            "SELECT MAX(x) FROM ("
            " SELECT COALESCE(json_extract(data, '$.time.completed'), 0) AS x"
            "   FROM message WHERE session_id = ?"
            " UNION SELECT time_created AS x FROM message WHERE session_id = ?"
            " UNION SELECT time_updated AS x FROM message WHERE session_id = ?"
            " UNION SELECT time_created AS x FROM part WHERE session_id = ?"
            " UNION SELECT time_updated AS x FROM part WHERE session_id = ?)",
            (child, child, child, child, child)).fetchone()
        return int(r[0] or 0) if r else 0
    except Exception:
        return 0


def _llama_child_terminal(con, child: str, after_ms: int) -> bool:
    """Child structurally finished: its last assistant message has
    data.time.completed >= after_ms, or a step-finish part exists after."""
    try:
        r = con.execute(
            "SELECT json_extract(data, '$.time.completed') FROM message "
            "WHERE session_id = ? AND json_extract(data, '$.role')='assistant' "
            "ORDER BY time_created DESC LIMIT 1", (child,)).fetchone()
        if r and r[0] and int(r[0]) >= after_ms:
            return True
    except Exception:
        pass
    try:
        r = con.execute(
            "SELECT 1 FROM part WHERE session_id = ? AND "
            "json_extract(data, '$.type')='step-finish' AND "
            "time_created >= ? LIMIT 1", (child, after_ms)).fetchone()
        if r:
            return True
    except Exception:
        pass
    return False


def _llama_owner_state(con, owner: str, child: str, p_status: str,
                       part_start: int, now_ms: int):
    """(state, tag, src) for one owner (part:<pid> or session:<sid>).
    Lifecycle rules (design 2, tool-loop caveat a/b/c):
      - part status 'error'        -> finished/error immediately (error counts
                                      as finished; fixes error-shown-as-running);
      - any owned task in flight   -> started (live elapsed, never stale);
      - part 'completed'           -> finished (definitive structural end);
      - child terminal marks      -> finished (db fallback 2: message
                                      time.completed / step-finish part);
      - owned tasks all ended and  -> finished (grace c; no new task for
        now - last_end >= 5 s        LLAMA_GRACE_MS);
      - otherwise                  -> started.
    src = 'llama' iff the task registry owns any task for this owner."""
    owned = _llama_owned_tasks(owner)
    src = "llama" if owned else "db"
    if p_status == "error":
        return "finished", "error", src
    if any(not t.get("end") for t in owned):
        return "started", "started", src
    if p_status == "completed":
        return "finished", "finished", src
    if p_status in ("running", "pending"):
        # opencode writes 'completed' on the task part at the real end. A
        # child's step-finish / completed message only ends ONE step (every
        # tool loop has them), and llama goes quiet for >5 s whenever a tool
        # runs, so those signals flagged subagents finished after their first
        # turn (08:30:19 run reported done at 08:30:49; it ran to 09:04).
        # Only a part orphaned by an interrupted shutdown is overridden: no
        # child activity for LLAMA_ORPHAN_MS. The anchor must EXIST: a task-
        # tool part row appears in the DB at time_created as a stub, before
        # opencode writes time.start / input / child id (2026-10-02: fresh
        # parts with no data were instantly 'finished' at their creation
        # second, ringing a 0 s "finished" notification that then burned the
        # eid, so the true finish minutes later never rang).
        last = _llama_child_last_activity(con, child) if child else 0
        anchor = max(last, part_start or 0)
        if anchor and now_ms - anchor >= LLAMA_ORPHAN_MS:
            return "finished", "finished", src
        return "started", "started", src
    if child and _llama_child_terminal(con, child, part_start or 0):
        return "finished", "finished", src
    last_end = max([t.get("end") or 0 for t in owned] or [0])
    if last_end and now_ms - last_end >= LLAMA_GRACE_MS:
        return "finished", "finished", src
    return "started", "started", src


# ── llama telemetry (context size / gen-speed / resource sampler) ──────────────
TELE_INTERVAL_S = 4.0
MAX_SERIES = 600          # 40 min of history at 4 s
_VRAM_CACHE = None        # (queried_at, pid, mb)


def _tps(prev, cur) -> float:
    """Tokens/sec of generation between two /slots samples. Uses the slot's
    n_decoded counter (grows by 1 per generated token). 0 unless that counter
    advanced; a fresh task resets it to 0, which reads as no growth.

    Accepts both a raw /slots dict (wall_ms key) and a stored series point
    (t key) as the previous sample."""
    if not prev or not cur:
        return 0.0
    p_wall = prev.get("wall_ms") or prev.get("t") or 0
    c_wall = cur.get("wall_ms") or cur.get("t") or 0
    dt = (c_wall - p_wall) / 1000.0
    d = (cur.get("decoded_tokens") or 0) - (prev.get("decoded_tokens") or 0)
    if dt <= 0 or d <= 0:
        return 0.0
    return d / dt


def _ptps(prev, cur) -> float:
    """Tokens/sec of prompt *processing* (n_prompt_tokens_processed) between two
    /slots samples -- the prefill rate. Spikes while a request's prompt is being
    evaluated, ~0 during steady decode or when idle. 0 unless that counter
    advanced (it resets to 0 on each new task, like decoded_tokens)."""
    if not prev or not cur:
        return 0.0
    p_wall = prev.get("wall_ms") or prev.get("t") or 0
    c_wall = cur.get("wall_ms") or cur.get("t") or 0
    dt = (c_wall - p_wall) / 1000.0
    d = (cur.get("processed_tokens") or 0) - (prev.get("processed_tokens") or 0)
    if dt <= 0 or d <= 0:
        return 0.0
    return d / dt


def _compaction_detected(prev, cur) -> bool:
    """True when the KV-cache/context shrank between two /slots samples —
    either the prompt token count dropped sharply, or the task changed and the
    context got shorter. Pure helper (unit-testable)."""
    if not prev or not cur:
        return False
    pct = (prev.get("prompt_tokens") or 0)
    cct = (cur.get("prompt_tokens") or 0)
    if pct and cct < pct * 0.8:
        return True
    p_task, c_task = prev.get("id_task"), cur.get("id_task")
    if p_task is not None and c_task != p_task and cct < pct:
        return True
    return False


def llama_vram_mb(pid) -> float | None:
    """VRAM used by the llama process, from the Windows GPU process-memory
    perf counter (instance `pid_<pid>_*`). ~1 s query via powershell; the result
    is cached for 30 s. Returns None if unavailable. On Linux: the process's
    DRM fdinfo (amdgpu) or nvidia-smi."""
    global _VRAM_CACHE
    now = time.time()
    if _VRAM_CACHE and now - _VRAM_CACHE[0] < 30 and _VRAM_CACHE[1] == pid:
        return _VRAM_CACHE[2]
    mb = None
    if pid and os.name != "nt":
        mb = hawk_linux.process_vram_mb(pid)
    elif pid:
        try:
            # -EncodedCommand avoids powershell re-parsing the counter path
            # (the `(*)` wildcard breaks -Command string parsing).
            ps = ('$s=(Get-Counter -Counter "\\GPU Process Memory(*)\\Local Usage" '
                  '-ErrorAction SilentlyContinue).CounterSamples '
                  '| Where-Object { $_.InstanceName -like "pid_%d_*" } '
                  '| Select-Object -First 1; if ($s) { $s.CookedValue }') % pid
            enc = base64.b64encode(ps.encode("utf-16-le")).decode("ascii")
            out = subprocess.run(["powershell", "-NoProfile", "-NonInteractive",
                                  "-EncodedCommand", enc],
                                 capture_output=True, text=True, encoding="utf-8",
                                 errors="replace", timeout=4, creationflags=subprocess.CREATE_NO_WINDOW).stdout
            for ln in out.strip().splitlines():
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    mb = float(ln) / 1048576.0  # counter reports bytes -> MB
                    break
                except ValueError:
                    continue
        except Exception as e:
            coordinator.log_debug("llama_vram_mb: %s" % e)
    _VRAM_CACHE = (now, pid, mb)
    return mb


# System-wide GPU engine utilisation, ported from the working AHK monitor
# (telemetry_monitor_fast.ahk): the LARGEST single-engine value of
# '\GPU Engine(*)\Utilization Percentage' across ALL processes (0-100). The
# per-pid WDDM view stays 0 on this AMD driver mid-generation, so the all-engine
# max is the real, observable GPU load. The PDH reader itself lives in
# coordinator.py (single source of truth); we delegate to it.


def gpu_system_max(state=None) -> float:
    """Highest single GPU-engine utilisation (%) across all processes, or 0.

    Delegates to coordinator.gpu_engine_max() — the shared PDH reader used by
    both the dashboard telemetry loop and the coordinator's llama_gpu_util()."""
    try:
        return coordinator.gpu_engine_max()
    except Exception as e:
        coordinator.log_debug("gpu_system_max: %s" % e)
        return 0.0


def _slots_decoded(cfg):
    """Generated-token count (next_token.n_decoded) from llama /slots, or None.
    Fetched here (not via coordinator.llama_slots) so the dashboard owns its
    generation-rate signal without touching the worker's slot parser."""
    import urllib.request
    try:
        with urllib.request.urlopen(coordinator.llama_api_base(cfg) + "/slots",
                                     timeout=2.0) as r:
            d = json.loads(r.read().decode("utf-8", "replace"))
        if isinstance(d, list) and d:
            nt = d[0].get("next_token")
            if isinstance(nt, list):
                nt = nt[0] if nt else {}
            if isinstance(nt, dict):
                return int(nt.get("n_decoded") or 0)
    except Exception:
        return None
    return None


def _tele_sample() -> None:
    """One telemetry sample: /slots context+speed, CPU frac, RAM, VRAM, GPU
    util, disk IO rates. Appends to the in-memory series (thread-safe)."""
    global _tele_series, _tele_compactions, _tele_meta
    cfg = coordinator.load_config()
    pid = coordinator.llama_pid(cfg)
    slots = coordinator.llama_slots(cfg) if pid else None
    ls = llama_status()
    now_ms = utcms()
    cur = dict(slots) if slots else None
    if cur is not None:
        cur["wall_ms"] = now_ms
        dec = _slots_decoded(cfg)
        if dec is not None:
            cur["decoded_tokens"] = dec
    with _tele_lock:
        global _tele_last_raw
        prev = _tele_series[-1] if _tele_series else None
        tps = 0.0
        ptps = 0.0
        if cur:
            if _tele_last_raw is not None:
                tps = _tps(_tele_last_raw, cur)
                ptps = _ptps(_tele_last_raw, cur)
            _tele_last_raw = cur
        elif _tele_last_raw is not None:
            _tele_last_raw = None  # llama went idle; next busy sample starts fresh
        compact = _compaction_detected(prev, cur) if cur and prev else False
        comp_from = prev["tokens"] if prev else 0
        tokens = cur["prompt_tokens"] if cur else (prev["tokens"] if prev else 0)
        # Running count of tokens processed: on the first busy sample the total
        # is seeded with the current context (a dashboard restart mid-session
        # keeps the count from being 0); afterwards add context growth and
        # decoded growth since the last sample, clamped so a context reset
        # (compaction, new task) never subtracts.
        prev_ctx = _tele_meta.get("ctx_tokens")
        total_add = 0
        if cur and prev_ctx is None:
            total_add = tokens
        elif prev_ctx is not None and tokens > prev_ctx:
            total_add = tokens - prev_ctx
        _tele_meta["ctx_tokens"] = tokens
        if cur is not None and cur.get("decoded_tokens") is not None:
            dec = cur["decoded_tokens"]
            prev_dec = _tele_meta.get("decoded_tokens")
            if prev_dec is not None and dec > prev_dec:
                total_add += dec - prev_dec
            _tele_meta["decoded_tokens"] = dec
        elif not cur:
            _tele_meta.pop("decoded_tokens", None)
        _tele_meta["total_tokens"] = _tele_meta.get("total_tokens", 0) + total_add
        cpu_frac = ls.get("cpu_frac") or 0.0
        cpu_pct = _cpu_pct(cpu_frac)
        _tele_meta["cpu_frac"] = cpu_frac
        _tele_meta["cpu_pct"] = cpu_pct
        _tele_meta["pid"] = pid
        if cur:
            _tele_meta["n_ctx"] = cur.get("n_ctx") or 0
            _tele_meta["processing"] = bool(cur.get("is_processing"))
        if not cur:
            _tele_meta["processing"] = False
        # System-wide RAM/VRAM in use (Task Manager style). Fall back to the
        # llama process' own numbers only if the system read is unavailable.
        ram_mb = coordinator.system_ram_used_mb()
        if ram_mb is None:
            memio = coordinator.llama_mem_io(pid) if pid else None
            if memio:
                ram_mb = round(memio[0] / 1024.0 / 1024.0, 1)
        _tele_meta["ram_mb"] = ram_mb
        vram = coordinator.gpu_vram_used_mb()
        if vram is None:
            vram = llama_vram_mb(pid)
        _tele_meta["vram_mb"] = vram
        # WDDM's GPU Engine counter does not attribute AMD ROCm/HIP compute to
        # the llama process (per-pid reads are 0 even mid-generation), so use
        # the AHK-style system-wide max engine % — the same signal the user's
        # telemetry_monitor_fast.ahk dashboard showed.
        gpu_pct = gpu_system_max()
        _tele_meta["gpu_pct"] = gpu_pct
        _tele_series.append({"t": now_ms, "tokens": tokens,
                              "tps": round(tps, 2), "ptps": round(ptps, 2),
                              "compact": bool(compact),
                              "cpu_frac": round(cpu_frac, 3), "cpu_pct": cpu_pct,
                              "ram_mb": ram_mb, "vram_mb": vram,
                              "gpu_pct": gpu_pct})
        if compact:
            _tele_compactions.append({"t": now_ms, "from": comp_from, "to": tokens})
        if len(_tele_series) > MAX_SERIES:
            _tele_series = _tele_series[-MAX_SERIES:]
        if len(_tele_compactions) > 100:
            _tele_compactions = _tele_compactions[-100:]
        pt = _tele_series[-1]
    try:
        telemetry_record(pt)
    except Exception as e:
        coordinator.log_debug("telemetry_record: %s" % e)


SUBAGENT_PUSH_LOOKBACK_MS = 2 * 3600 * 1000


def _event_push_sample():
    """Watermark detection for the push kinds the DASHBOARD owns (A6):
    commit / milestone / subagent / permission. The coordinator/monitor owns
    escalation / plan_done / continue / confirm_done. Every push goes through
    notify.record_event, which applies the kinds gate, per-kind cooldown and
    eid dedupe, so the two owners can never double-push the same event."""
    from . import notify
    cfg = coordinator.load_config()
    try:
        pd = str(cfg.get("project_dir") or "")
        if pd:
            # --- commit: watermark = last pushed git head ---
            head = coordinator.git_head(pd)
            if head:
                wsha = notify.get_watermark(pd, "commit")
                if wsha is None:
                    notify.set_watermark(pd, "commit", head)  # first sample = baseline
                elif head != wsha:
                    evs = git_log_events(pd, 5)
                    top = evs[0] if evs else None
                    if top:
                        notify.record_event(
                            cfg, "commit", top["title"],
                            top.get("detail") or top["title"],
                            eid="commit:%s" % top["id"],
                            meta={"project": pd, "sha": top["id"]})
                    notify.set_watermark(pd, "commit", head)
        # --- milestone + subagent: watermark = max event time (monotonic) ---
        sid = str(cfg.get("session_id") or "")
        if sid:
            pushed = set(notify.load_push_state().get("pushed_eids") or [])
            con = coordinator.connect_db(coordinator.db_path())
            try:
                # Subagents are scanned across ALL sessions (not just the
                # focused one): scanning only the focused session meant a
                # focus switch (e.g. dashboard auto-select on start) dumped
                # that session's older finished events past the shared
                # watermark in one burst, while other sessions never rang.
                since = utcms() - 2 * 86400 * 1000
                for kind, fn in (("milestone", lambda c: milestone_events(c, sid)),
                                 ("subagent", lambda c: subagent_events(c, "", since))):
                    evs = fn(con)
                    if not evs:
                        continue
                    wt = int(notify.get_watermark("home", kind) or 0)
                    # Subagent finishes are detected with a lag, so one can
                    # land behind a newer start that already moved the
                    # watermark; look back and let the eid dedupe decide.
                    lb = SUBAGENT_PUSH_LOOKBACK_MS if kind == "subagent" else 0
                    fresh = [e for e in evs if int(e.get("time") or 0) > wt - lb]
                    if kind == "subagent":
                        # Subagent push covers BOTH walls (update 2026-09-25):
                        # the started event rings the moment a task launches
                        # (time = started wall) and the finished/error event
                        # rings at true release (time = end wall). The eid
                        # carries the state so the finish is never deduped by
                        # the start, and the monotonic start->end time bump
                        # keeps the finish wall fresh past the watermark.
                        fresh = [e for e in fresh
                                 if e.get("state") in ("started", "finished")]
                        # Wait for the subagent name to load before ringing
                        # the START event: an unnamed start would push
                        # "subagent subagent:" and, once the watermark
                        # advances past it, never re-push with the real name.
                        # Dropping it from `fresh` (not just the push) keeps
                        # its start wall below the watermark so the next poll
                        # retries; finished/error events always ring.
                        fresh = [e for e in fresh
                                 if not (e.get("state") == "started"
                                         and not e.get("atype"))]
                        # A finished ring needs a real run: start wall > 0.
                        # (A stub or completed-without-start part would ring a
                        # lying "0s finished" and burn the finish eid — the
                        # 2026-10-02 bug. Leave it for a later vote where the
                        # real end wall exists.)
                        fresh = [e for e in fresh
                                 if not (e.get("state") == "finished"
                                         and not e.get("start"))]
                    if fresh:
                        for e in fresh:
                            title = e.get("title") or kind
                            if kind == "subagent":
                                # ntfy body mirrors the events-tab row: the
                                # state chip verbatim, then the event's own
                                # text (clock range / duration / tokens).
                                stw = e.get("tag") or e.get("state") or "started"
                                chip = {"started": "started", "finished": "finished",
                                        "error": "error"}.get(stw, stw)
                                body = "%s\n%s" % (chip, e.get("detail") or title)
                                # Full part id: the 8-char display id
                                # collides between subagents (prt_0f77...).
                                eid = "%s:%s:%s" % (
                                    kind, e.get("mid") or e.get("id"),
                                    e.get("state") or "started")
                                legacy = "%s:%s:%s" % (
                                    kind, e.get("id"), e.get("state") or "started")
                                if legacy != eid and legacy in pushed:
                                    continue  # pushed before the eid fix
                            else:
                                body = e.get("detail") or title
                                eid = "%s:%s" % (kind, e.get("id") or e.get("mid"))
                            if kind == "milestone":
                                meta = {"session": sid,
                                        "state": e.get("kind") or "",
                                        "tests": e.get("tests")}
                            else:
                                meta = {"session": sid, "child": e.get("child") or "",
                                        "state": e.get("state") or ""}
                            notify.record_event(cfg, kind, title, body,
                                                eid=eid, meta=meta)
                        notify.set_watermark("home", kind,
                                             max([wt] + [int(e.get("time") or 0)
                                                         for e in fresh]))
            finally:
                con.close()
        # --- permission: last 100 JSONL lines; eid dedupe covers re-push ---
        p = coordinator.home_dir() / "permission_events.jsonl"
        if p.exists():
            try:
                lines = p.read_text("utf-8", errors="replace").splitlines()[-100:]
            except OSError:
                lines = []
            for ln in lines:
                try:
                    d = json.loads(ln)
                except Exception:
                    continue
                k = d.get("kind")
                if k not in ("permission_accept", "question_answer"):
                    continue
                target = str(d.get("target") or "")
                sess = str(d.get("session") or "")
                verb = "auto-accept" if k == "permission_accept" else "question answered"
                title = "permission %s: %s" % (verb, short(target.split(",")[0], 60))
                if sess:
                    title += " · %s" % short(sess, 12)
                notify.record_event(
                    cfg, "permission", title, (target or title)[:220],
                    eid="perm:%s:%s" % (k, str(d.get("id") or "")),
                    meta={"session": sess, "kind": k})
    except Exception as e:
        coordinator.log_debug("event_push: %s" % e)


def _tele_loop():
    _tele_restore_state()
    while True:
        try:
            _tele_sample()
        except Exception as e:
            coordinator.log_debug("telemetry: %s" % e)
        try:
            _event_push_sample()
        except Exception as e:
            coordinator.log_debug("event_push: %s" % e)
        time.sleep(TELE_INTERVAL_S)


def llama_telemetry_payload() -> dict:
    """Snapshot for /api/llama: live resource tiles + the sampled series
    (context tokens / t/s / resource samples / compaction markers) for the
    charts. vram_total_mb / ram_total_mb pin the chart Y-axes and the
    used/total/% tiles."""
    vram_total = coordinator.gpu_vram_total_mb()
    ram_total = coordinator.system_ram_total_mb()
    with _tele_lock:
        last = _tele_series[-1] if _tele_series else {}
        # Values that are also charted come from the latest series point so the
        # text tiles and the graphs always display identical numbers; only
        # non-charted state (pid / n_ctx / processing / disk rates) comes from
        # _tele_meta.
        vram_mb = last.get("vram_mb", _tele_meta.get("vram_mb"))
        ram_mb = last.get("ram_mb", _tele_meta.get("ram_mb"))
        return {
            "pid": _tele_meta.get("pid"),
            "n_ctx": _tele_meta.get("n_ctx", 0),
            "processing": bool(_tele_meta.get("processing")),
            "cpu_frac": last.get("cpu_frac", _tele_meta.get("cpu_frac", 0.0)),
            "cpu_pct": last.get("cpu_pct", _tele_meta.get("cpu_pct", 0)),
            "ram_mb": ram_mb,
            "vram_mb": vram_mb,
            "gpu_pct": last.get("gpu_pct", _tele_meta.get("gpu_pct", 0.0)),
            "tps": last.get("tps", 0.0),
            "ptps": last.get("ptps", 0.0),
            "context_tokens": last.get("tokens", 0),
            "total_tokens": _tele_meta.get("total_tokens", 0),
            "vram_total_mb": vram_total,
            "ram_total_mb": ram_total,
            "vram_pct": round(100.0 * vram_mb / vram_total, 1) if (vram_mb and vram_total) else 0,
            "ram_pct": round(100.0 * ram_mb / ram_total, 1) if (ram_mb and ram_total) else 0,
            "hw_sticks": hw_ram_sticks(),
            "hw_ram": hw_ram_sticks_info(),
            "hw_disks": hw_disk_activity(),
            "hw_gpus": hw_gpus(),
            "series": list(_tele_series),
            "compactions": list(_tele_compactions),
        }


# ── Hardware overview (gauge row): RAM sticks, per-GPU VRAM, disk activity ──
# The gauge row draws one stick glyph per physically installed DIMM, one glyph
# per GPU with dedicated VRAM (each filled with that GPU's real usage), and one
# glyph per physical disk (each filled with the disk's REAL-TIME active % —
# 100 - %Idle Time, Task-Manager semantics; NOT storage-used %). Static
# hardware (stick count/capacities, adapter totals, disk->letters mapping) is
# probed once via PowerShell and cached; live readings come from the cheap
# per-second PDH sampler in coordinator.
_HW_LOCK = threading.Lock()
_HW = {"sticks": None, "sticks_info": [], "gpu_totals": None,
       "disk_inv": None, "disks": [], "disks_at": 0.0,
       "gpus": [], "gpus_at": 0.0}
HW_TTL_S = 5.0     # live readings (disk activity, GPU usage) cached 5 s
MIN_DISK_BYTES = 1024 ** 3   # disks smaller than 1 GB are ignored (card readers)


def _hw_sticks_sync() -> tuple:
    """Win32_PhysicalMemory via one PowerShell CIM query. Returns
    (count, [{locator, gb, speed}]) — (None, []) on any failure (no
    PowerShell / WMI absent / VM)."""
    if os.name != "nt":
        return None, []
    try:
        cmd = ('powershell.exe -NoProfile -NonInteractive -Command '
               '"$m = Get-CimInstance Win32_PhysicalMemory; $m.Count; '
               '$m | ForEach-Object { \'{0}|{1}|{2}\' -f $_.DeviceLocator, '
               '$_.Capacity, $_.ConfiguredClockSpeed }"')
        out = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                             creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                             timeout=10).stdout
        lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
        if not lines:
            return None, []
        n = int(lines[0])
        if n <= 0:
            return None, []
        info = []
        for ln in lines[1:]:
            parts = ln.split("|")
            if len(parts) != 3:
                continue
            try:
                gb = round(int(parts[1]) / (1024 ** 3), 1)
            except ValueError:
                continue
            try:
                speed = int(parts[2] or 0) or 0
            except ValueError:
                speed = 0  # older sticks can report no configured clock speed
            info.append({"locator": parts[0], "gb": gb, "speed": speed})
        return n, info
    except Exception:
        return None, []


def _hw_sticks() -> tuple:
    """cached (count, info); computes once under the lock."""
    if _HW.get("sticks") is None:
        with _HW_LOCK:
            if _HW.get("sticks") is None:
                _HW["sticks"], _HW["sticks_info"] = _hw_sticks_sync()
    return _HW["sticks"], _HW["sticks_info"]


def hw_ram_sticks() -> int | None:
    """Physically installed RAM stick count (static hardware, cached)."""
    return _hw_sticks()[0]


def hw_ram_sticks_info() -> list[dict]:
    """[{locator, gb, speed}] real per-stick data; [] when the query failed."""
    return [dict(s) for s in _hw_sticks()[1]]


def _hw_gpu_totals_sync() -> list[dict]:
    """[{name, total_mb}] per adapter, in the same order as the per-adapter
    usage rows (see coordinator.gpu_adapter_totals)."""
    return coordinator.gpu_adapter_totals()


def _hw_disk_inventory_sync() -> dict:
    """disk index ('0'..'N') -> {model, letters, size} via Win32_DiskDrive
    (Size = total bytes) and its partitions' drive letters. Empty on failure;
    the letters fallback keeps the glyphs labelable ('Disk n') when the
    mapping is unavailable."""
    if os.name != "nt":
        return hawk_linux.disk_inventory()
    inv: dict = {}
    if os.name != "nt":
        return inv
    try:
        cmd = ('powershell.exe -NoProfile -NonInteractive -Command '
               '"Get-CimInstance Win32_DiskDrive | ForEach-Object { '
               '$d = $_; $ps = Get-CimAssociatedInstance -InputObject $d '
               '-ResultClassName Win32_DiskPartition -ErrorAction SilentlyContinue; '
               '$ls = @(); foreach ($p in $ps) { $ld = Get-CimAssociatedInstance '
               '-InputObject $p -ResultClassName Win32_LogicalDisk '
               '-ErrorAction SilentlyContinue; '
               'if ($ld.DeviceID) { $ls += $ld.DeviceID } }; '
               '\'{0}|{1}|{2}|{3}\' -f $d.Index, $d.Model, ($ls -join \',\'), '
               '$d.Size }"')
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                             creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                             timeout=15).stdout
        for ln in res.splitlines():
            ln = ln.strip()
            parts = ln.split("|")
            if len(parts) < 2:
                continue
            idx = parts[0].strip()
            model = parts[1].strip()
            letters = [l for l in (parts[2] or "").split(",") if l]
            try:
                size = int(parts[3] or 0)
            except ValueError:
                size = 0
            inv[idx] = {"model": model, "letters": letters, "size": size}
    except Exception:
        return inv
    return inv


def hw_gpus() -> list[dict]:
    """Per-GPU snapshot: [{name, total_mb, used_mb, pct}] — one entry per GPU
    with REAL dedicated VRAM (registry qwMemorySize > 0). Pseudo/virtual
    adapters — iGPU/UMA, virtual display adapters etc. — are excluded: they
    have no VRAM of their own, so no card is drawn for them. pct is that GPU's
    PROCESSING (engine) utilisation %: its busiest engine (3D/Copy/VideoEncode/
    VideoDecode) across all processes sharing the adapter — the same semantics
    as the all-engine gauge, scoped per adapter. used_mb is the PDH per-adapter
    dedicated usage; both PDH rows are LUID-grouped and paired to the registry
    totals by WDDM enumeration order (matches on this box). Cached 5 s."""
    if _HW.get("gpu_totals") is None:
        with _HW_LOCK:
            if _HW.get("gpu_totals") is None:
                _HW["gpu_totals"] = _hw_gpu_totals_sync()
    eng = coordinator.gpu_adapters_engine_pct()
    inst = coordinator.gpu_adapters_usage_mb()
    now = time.time()
    with _HW_LOCK:
        if _HW.get("gpus_at", 0.0) + HW_TTL_S > now:
            return [dict(g) for g in _HW["gpus"]]
        totals = _HW.get("gpu_totals") or []
        out = []
        for i, t in enumerate(totals):
            if not t.get("total_mb", 0.0):
                continue  # no dedicated VRAM -> no graphics card icon
            out.append({
                "name": t["name"],
                "total_mb": round(t["total_mb"], 1),
                "used_mb": round(inst[i][1], 1) if i < len(inst) else 0.0,
                "pct": round(eng[i][1], 1) if i < len(eng) else 0.0,
            })
        # never cache a cold-start empty (background PDH sampler warms up
        # ~1 s): hold the last good snapshot, else return [] uncached
        prev = _HW.get("gpus")
        if not out and prev:
            return [dict(g) for g in prev]
        if out:
            _HW["gpus"] = out
            _HW["gpus_at"] = now
        return [dict(g) for g in out]


def hw_disk_activity() -> list[dict]:
    """Real-time disk activity % per physical disk: [{label, pct}] where
    pct = 100 - %Idle Time from the PDH sampler (the disk's own active % —
    this is usage of the drive in real time, NOT its storage used-space).
    Only PHYSICAL disks with at least MIN_DISK_BYTES (1 GB) of total storage
    are shown — empty card readers and sub-GB sticks are ignored; logical
    volumes are never enumerated (the gauge is per physical disk, not per
    volume). Label is the disk's drive letters (e.g. 'C:') or 'Disk n'.
    [] when the counter is absent. Cached 5 s."""
    if _HW.get("disk_inv") is None:
        with _HW_LOCK:
            if _HW.get("disk_inv") is None:
                _HW["disk_inv"] = _hw_disk_inventory_sync()
    inst = coordinator.physical_disk_active_pct()
    now = time.time()
    with _HW_LOCK:
        if _HW.get("disks_at", 0.0) + HW_TTL_S > now:
            return [dict(d) for d in _HW["disks"]]
        inv = _HW["disk_inv"]
        out = []
        for idx, pct in inst:
            info = inv.get(idx) or {}
            if info and (info.get("size") or 0) < MIN_DISK_BYTES:
                continue  # <1 GB storage: card reader / sub-GB stick, not real
            letters = info.get("letters") or []
            label = ", ".join(letters) if letters else ("Disk " + idx)
            out.append({"label": label, "pct": pct})
        # never cache a cold-start empty (background PDH sampler warms up
        # ~1 s): hold the last good snapshot, else return [] uncached
        prev = _HW.get("disks")
        if not out and prev:
            return [dict(d) for d in prev]
        if out:
            _HW["disks"] = out
            _HW["disks_at"] = now
        return [dict(d) for d in out]


_tele_lock = threading.Lock()
_tele_series = []
_tele_compactions = []
_tele_last_raw = None   # previous raw /slots sample for the tps diff (reset on idle)
_tele_meta = {
    "pid": None, "n_ctx": 0, "processing": False, "cpu_frac": 0.0,
    "cpu_pct": 0, "ram_mb": None, "vram_mb": None, "gpu_pct": 0.0,
    "total_tokens": 0,
}


# ── Telemetry persistence (SQLite) + server-side downsampling ────────────────
# The in-memory _tele_series keeps ~40 min (600 pts @ 4 s). The SQLite store
# keeps 3 days so the dashboard can render 1h/6h/24h views; each poll returns a
# bucket-averaged series of ~300 points so the front-end gets one flat shape
# regardless of window. A fresh connection per op keeps it thread-safe.
TELE_DB_NAME = "hawk_telemetry.db"
TELE_RETENTION_S = 30 * 86400         # keep 30 days
TELE_TARGET_POINTS = 300
TELE_WINDOWS = {"5m": 300, "15m": 900, "1h": 3600, "6h": 21600, "24h": 86400,
                "1d": 86400, "7d": 604800, "30d": 2592000}
TELE_DEFAULT_WINDOW_S = 3600
_tele_purge_at = 0.0
_POINT_KEYS = ("t", "tokens", "tps", "ptps", "compact", "cpu_frac", "cpu_pct",
                "ram_mb", "vram_mb", "gpu_pct", "ctx_tokens", "decoded_tokens")


def _window_seconds(w) -> int:
    """'5m'/'15m'/'1h'/'6h'/'24h'/'1d'/'7d'/'30d' -> seconds; anything else -> default."""
    if isinstance(w, (int, float)):
        return int(w)
    return TELE_WINDOWS.get(str(w).strip().lower(), TELE_DEFAULT_WINDOW_S)


def _tele_db_path() -> Path:
    return coordinator.home_dir() / TELE_DB_NAME


def _tele_db():
    con = sqlite3.connect(str(_tele_db_path()), timeout=5)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute(
        "CREATE TABLE IF NOT EXISTS telemetry ("
        " ts INTEGER PRIMARY KEY, cpu_frac REAL, cpu_pct REAL, "
        " ram_mb REAL, vram_mb REAL, gpu_pct REAL, tps REAL, ptps REAL, "
        " tokens INTEGER, compact INTEGER, pid INTEGER, "
        " total_tokens INTEGER, ctx_tokens INTEGER, decoded_tokens INTEGER, "
        " session_id TEXT)")
    for col in ("total_tokens", "ctx_tokens", "decoded_tokens"):
        try:
            con.execute("ALTER TABLE telemetry ADD COLUMN %s INTEGER" % col)
        except sqlite3.OperationalError:
            pass  # already present
    try:
        con.execute("ALTER TABLE telemetry ADD COLUMN session_id TEXT")
    except sqlite3.OperationalError:
        pass  # already present
    con.execute(
        "CREATE TABLE IF NOT EXISTS session_tokens ("
        " ts INTEGER, session_id TEXT, tokens_input INTEGER, "
        " tokens_output INTEGER, PRIMARY KEY (ts, session_id))")
    _llama_tasks_table(con)
    con.commit()
    return con


def _row_to_point(r) -> dict:
    # r: (ts, cpu_frac, cpu_pct, ram_mb, vram_mb, gpu_pct, tps, ptps, tokens,
    #     compact, pid, total_tokens, ctx_tokens, decoded_tokens) from the
    #     history SELECT.
    return {"t": r[0], "cpu_frac": r[1] or 0.0, "cpu_pct": r[2] or 0,
            "ram_mb": r[3], "vram_mb": r[4], "gpu_pct": r[5] or 0.0,
            "tps": r[6] or 0.0, "ptps": r[7] or 0.0, "tokens": r[8] or 0,
            "compact": bool(r[9]), "pid": r[10],
            "total_tokens": r[11] or 0,
            "ctx_tokens": r[12] or 0,
            "decoded_tokens": r[13] or 0}


def telemetry_record(sample: dict) -> None:
    """Persist one sampled point (called from _tele_sample each tick). Also
    snapshots the running token-count state so a dashboard restart resumes the
    total instead of reseeding it."""
    if not sample or not sample.get("t"):
        return
    # Tag the sample with the currently focused session (from config).
    sid = ""
    try:
        cfg = coordinator.load_config()
        sid = str(cfg.get("session_id") or "").strip()
    except Exception:
        pass
    con = _tele_db()
    try:
        con.execute(
            "INSERT OR REPLACE INTO telemetry "
            "(ts,cpu_frac,cpu_pct,ram_mb,vram_mb,gpu_pct,tps,ptps,tokens,compact,pid,"
            "total_tokens,ctx_tokens,decoded_tokens,session_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (int(sample["t"]), sample.get("cpu_frac") or 0.0,
              sample.get("cpu_pct") or 0, sample.get("ram_mb"),
              sample.get("vram_mb"), sample.get("gpu_pct") or 0.0,
              sample.get("tps") or 0.0, sample.get("ptps") or 0.0,
              int(sample.get("tokens") or 0), 1 if sample.get("compact") else 0,
              _tele_meta.get("pid"),
              _tele_meta.get("total_tokens", 0),
              _tele_meta.get("ctx_tokens", 0),
              _tele_meta.get("decoded_tokens", 0),
              sid or None))
        if sid:
            try:
                ocon = coordinator.connect_db(coordinator.db_path())
                row = ocon.execute(
                    "SELECT tokens_input, tokens_output FROM session WHERE id=?",
                    (sid,)).fetchone()
                ocon.close()
                if row:
                    con.execute(
                        "INSERT OR REPLACE INTO session_tokens "
                        "(ts, session_id, tokens_input, tokens_output) "
                        "VALUES (?,?,?,?)",
                        (int(sample["t"]), sid,
                         int(row[0] or 0), int(row[1] or 0)))
            except Exception:
                pass
        con.commit()
    finally:
        con.close()


def _tele_backfill_totals() -> None:
    """Recompute total_tokens/ctx_tokens for the WHOLE stored series in one
    deterministic pass (idempotent, replayed from the stored `tokens` context
    and `decoded_tokens` columns using the same accumulation as the sampler).
    Guarantees the series is monotonic even after the counter columns were
    added to a pre-existing history store, which keeps window deltas correct."""
    try:
        con = _tele_db()
        try:
            rows = con.execute(
                "SELECT ts, tokens, decoded_tokens, total_tokens "
                "FROM telemetry ORDER BY ts").fetchall()
            if not rows:
                return
            total = 0
            prev_ctx = None
            prev_dec = None
            base_ts = rows[0][0]
            updates = []
            for ts, tokens, dec, old_total in rows:
                add = 0
                if prev_ctx is None:
                    add = tokens or 0        # seed: first busy sample
                elif tokens > prev_ctx:
                    add = tokens - prev_ctx   # context growth
                prev_ctx = tokens
                if dec is not None and prev_dec is not None and dec > prev_dec:
                    add += dec - prev_dec     # decode growth
                if dec is not None:
                    prev_dec = dec
                total += add
                updates.append((total, tokens, ts))
            con.executemany(
                "UPDATE telemetry SET total_tokens=?, ctx_tokens=? WHERE ts=?",
                updates)
            con.commit()
            coordinator.log_debug("telemetry backfill: %d rows from ts=%s"
                                  % (len(updates), base_ts))
        finally:
            con.close()
    except Exception as e:
        coordinator.log_debug("telemetry backfill: %s" % e)


def _tele_restore_state() -> None:
    """Resume the token-count accumulator from the last persisted sample so a
    dashboard/hawk restart keeps counting (the total never resets). Falls back
    to the live-context seed when no persisted state exists yet."""
    _tele_backfill_totals()
    try:
        con = _tele_db()
        try:
            r = con.execute(
                "SELECT total_tokens, ctx_tokens, decoded_tokens FROM telemetry "
                "ORDER BY ts DESC LIMIT 1").fetchone()
        finally:
            con.close()
        if r and r[0] is not None and r[1] is not None:
            _tele_meta["total_tokens"] = int(r[0])
            _tele_meta["ctx_tokens"] = int(r[1])
            if r[2] is not None:
                _tele_meta["decoded_tokens"] = int(r[2])
    except Exception as e:
        coordinator.log_debug("telemetry restore: %s" % e)


def _downsample(points: list, target: int) -> list:
    """Bucket-average `points` (ascending by t) to ~target points. Level
    metrics (tokens) take the bucket's last value; rates/utilisations average;
    compact is OR'd. No-op when already <= target."""
    n = len(points)
    if n <= target:
        return points
    out = []
    for i in range(target):
        lo = int(i * n / target)
        hi = max(lo + 1, int((i + 1) * n / target))
        grp = points[lo:hi]
        m = len(grp)
        avg = lambda k: sum((g[k] or 0) for g in grp) / m
        out.append({
            "t": grp[-1]["t"],
            "tokens": grp[-1].get("tokens") or 0,
            "ctx_tokens": grp[-1].get("ctx_tokens") or 0,
            "decoded_tokens": grp[-1].get("decoded_tokens") or 0,
            "tps": round(avg("tps"), 2),
            "ptps": round(avg("ptps"), 2),
            "compact": any(g.get("compact") for g in grp),
            "cpu_frac": round(avg("cpu_frac"), 3),
            "cpu_pct": round(avg("cpu_pct")),
            "ram_mb": round(avg("ram_mb"), 1),
            "vram_mb": round(avg("vram_mb"), 1),
            "gpu_pct": round(avg("gpu_pct"), 1),
        })
    return out


def _purge_old(con) -> None:
    global _tele_purge_at
    now = time.time()
    if now - _tele_purge_at < 120:
        return
    _tele_purge_at = now
    try:
        con.execute("DELETE FROM telemetry WHERE ts < ?",
                    (utcms() - TELE_RETENTION_S * 1000,))
        con.commit()
    except Exception:
        pass


_db_total_tokens_cache: tuple = (0, 0.0)


def _db_total_tokens() -> int:
    """Sum of all sessions' tokens_input + tokens_output + tokens_reasoning from
    the opencode DB — the same source the per-subagent event/notify counts use,
    so the webui 'Total Tokens' figure lines up with them. Cached 30s to avoid
    repeated scans of the multi-GB DB."""
    global _db_total_tokens_cache
    now = time.time()
    if now - _db_total_tokens_cache[1] < 30:
        return _db_total_tokens_cache[0]
    total = _db_total_tokens_cache[0]  # fall back to last known value on error
    try:
        con = sqlite3.connect(str(coordinator.db_path()), timeout=30)
        try:
            r = con.execute(
                "SELECT COALESCE(SUM(tokens_input),0)+COALESCE(SUM(tokens_output),0)"
                "+COALESCE(SUM(tokens_reasoning),0) FROM session").fetchone()
            total = int(r[0] or 0) if r else 0
        finally:
            con.close()
    except Exception:
        pass
    _db_total_tokens_cache = (total, now)
    return total


def telemetry_history(window_s: int, target: int = TELE_TARGET_POINTS,
                      session_id: str = "") -> dict:
    """Downsampled telemetry for a window (seconds). Prefers the SQLite store;
    falls back to the in-memory series (first ~40 min) when the DB is still
    empty (fresh start / before it fills). Returns {points, window_s, raw,
    count} where points is the same shape the charts already consume.
    When session_id is given, only samples tagged with that session are used."""
    window_s = int(window_s)
    now_ms = utcms()
    cutoff = now_ms - window_s * 1000
    rows: list[dict] = []
    try:
        con = _tele_db()
        try:
            _purge_old(con)
            if session_id:
                rows = [_row_to_point(r) for r in con.execute(
                    "SELECT ts,cpu_frac,cpu_pct,ram_mb,vram_mb,gpu_pct,"
                    "tps,ptps,tokens,compact,pid,total_tokens,ctx_tokens,"
                    "decoded_tokens FROM telemetry "
                    "WHERE ts>=? AND session_id=? ORDER BY ts",
                    (cutoff, session_id)).fetchall()]
            else:
                rows = [_row_to_point(r) for r in con.execute(
                    "SELECT ts,cpu_frac,cpu_pct,ram_mb,vram_mb,gpu_pct,"
                    "tps,ptps,tokens,compact,pid,total_tokens,ctx_tokens,"
                    "decoded_tokens FROM telemetry "
                    "WHERE ts>=? ORDER BY ts", (cutoff,)).fetchall()]
        finally:
            con.close()
    except Exception as e:
        coordinator.log_debug("telemetry_history db: %s" % e)
        rows = []
    if len(rows) < 2:
        with _tele_lock:
            rows = sorted(
                ({k: p.get(k) for k in _POINT_KEYS} for p in _tele_series
                 if p.get("t") and p["t"] >= cutoff),
                key=lambda p: p["t"])
    pts = _downsample(rows, target)
    # Window-scoped token count: the running total is monotonic, so the amount
    # processed inside the selected range is the delta between the last and
    # first persisted samples in that range. Without persisted state (fresh
    # start / in-memory fallback) the live total is the best estimate.
    win_total = 0
    win_input = 0
    win_output = 0
    if rows:
        first = next((p.get("total_tokens") or 0 for p in rows
                      if p.get("total_tokens")), 0)
        last = next((p.get("total_tokens") or 0 for p in reversed(rows)
                     if p.get("total_tokens")), 0)
        if last and first:
            win_total = max(0, last - first)
        else:
            win_total = _tele_meta.get("total_tokens", 0)
        # Split into input (context growth) and output (decoded growth).
        # Sum the per-sample deltas, clamped so resets never subtract.
        prev_ctx = None
        prev_dec = None
        for p in rows:
            ctx = p.get("ctx_tokens") or 0
            dec = p.get("decoded_tokens") or 0
            if prev_ctx is not None and ctx > prev_ctx:
                win_input += ctx - prev_ctx
            if prev_dec is not None and dec > prev_dec:
                win_output += dec - prev_dec
            prev_ctx = ctx
            prev_dec = dec
    return {"points": pts, "window_s": window_s,
            "raw": len(rows), "count": len(pts),
            "total_tokens": win_total,
            "total_input_tokens": win_input,
            "total_output_tokens": win_output}


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



if __name__ == "__main__":
    sys.exit(main())
