"""Phone notifications for hawk coordinator escalations/questions.

Stdlib-only (urllib + uuid + json). Channel:

- ntfy: notify.ntfy.topic -> push to https://ntfy.sh/<topic> as plain text
  (no action buttons). The user replies by sending a message in the ntfy app
  on the same topic; the monitor polls the topic's JSON stream endpoint
  - a reply targets the sessions awaiting an answer (the escalations the user
    has seen); with none it targets every enabled monitored session
    (free message). Exactly one target is delivered 1:1; several targets push
    a numbered routing prompt and the reply waits in pending_ntfy_routes.json
    (a digit resolves it; a newer message supersedes it), so concurrent
    sessions never silently steal each other's replies.
  Escalations can also carry a screenshot the workflow produced: with
  notify.ntfy.attach_screenshots (default on) the newest recent image in the
  session's project dir (root, .playwright-mcp/, docs/screenshots/,
  screenshots/, .opencode/tmp/) is uploaded and attached to the push.
  Self-published message IDs are tracked so the coordinator never treats its
  own notifications as replies.

All phone pushes flow through record_event(kind, title, body, eid), which
applies the notify.ntfy.kinds gate (checkboxes; absent key falls back to the
default set), per-kind cooldowns (notify.ntfy.kind_cooldown_s) and eid
dedupe (event_push_state.json). A coalescable kind (COALESCE_KINDS) then
joins the open burst window if there is one: the first event of the window
pushes as usual, the rest are collected and ship together as a single
summary when the window closes, so a tree that finishes dozens of tasks at
once costs three messages per minute rather than dozens - which is what
keeps ntfy from rate-limiting the client (see cohort_push.json).
Events a gate drops are neither logged nor pushed; a coalesced event is
remembered as handled and delivered with the summary instead. The web UI
always shows every event regardless of the push gate.
"""

import json
import os
import time as _time
import urllib.error
import urllib.request
import threading
import uuid
from pathlib import Path
from urllib.parse import quote

from .coordinator import log, log_debug, load_config, extract_choices
from . import coordinator as _hawk

NOTIFY_LOG_CAP = 500
# eid dedupe memory; must cover every event inside the dashboard's subagent
# push lookback window (2 h), across all sessions.
PUSHED_EIDS_CAP = 1000
# Back-off between retries when ntfy.sh answers 429 (seconds). ntfy rate-limits
# per client (a burst, then ~1 request / 5 s) and hawk's own channel polling
# shares that budget, so both the send and delete paths wait for the bucket to
# refill instead of failing the publish.
NTFY_429_WAITS_S = [6, 12, 20]
# Back-compat alias for the delete path's historical name.
DELETE_429_WAITS_S = NTFY_429_WAITS_S

# Reply-poll pacing. The gap between reply polls is persisted *before* the
# request, so a failed poll still counts: the old code only saved the map
# after a successful parse, which meant every failure reset the 10 s throttle
# and the monitor re-polled on every loop tick (~1/s). That burst is what
# tripped ntfy's per-client rate limiter and temporarily blocked this
# machine's IP (it recurs whenever ntfy hiccupped). Consecutive failures now
# widen the gap so a 429 storm or a blackholed node is never hammered.
NTFY_POLL_MIN_GAP_S = 10                    # normal spacing between reply polls
# Gap after 1, 2, 3, 4+ consecutive failures. Capped at 2 min: long enough
# that a dead node is left alone, short enough that replies resume quickly
# once ntfy comes back.
NTFY_POLL_BACKOFF_S = [10, 30, 60, 120]
# An ntfy Retry-After header may demand a longer wait than our schedule.
NTFY_POLL_RETRY_AFTER_CAP_S = 600

# Event taxonomy: everything the phone can be notified about. The web UI
# renders one checkbox per kind (gate the phone push only; the web timeline
# and sent/received list always show all kinds).
EVENT_KINDS = ["commit", "milestone", "escalation", "plan_done", "continue",
               "nudge", "confirm_done", "permission", "subagent", "llama"]
# Pushed when notify.ntfy.kinds is absent from config (matches pre-unification
# behavior: escalations + build-plan-complete + manual tests).
DEFAULT_NOTIFY_KINDS = ["escalation", "plan_done", "test"]
# Chatty kinds get a minimum gap between phone pushes (seconds); others push
# immediately. Overridable via notify.ntfy.kind_cooldown_s.
# NOTE on subagent: it is the chattiest kind (a parallel subagent tree can
# finish several at once -- 74 pushes in 8 min, ~400 ms apart, earned ntfy's
# first real 429s), but a cooldown here would break the deliberate intent that
# a subagent's started and finished both ring (see subagent_state_eids in the
# selftest). Rate-limiting subagent pushes is therefore a product decision, not
# a default; the poll-side pacing is handled in poll_ntfy_replies instead.
DEFAULT_KIND_COOLDOWNS_S = {"commit": 60, "continue": 60, "nudge": 60}

# ---- burst coalescing -----------------------------------------------------
# A parallel subagent tree can finish many tasks at once: 86 subagent pushes in
# 4 minutes (62 in a single minute, 394 ms apart) is what earns ntfy's
# per-client rate limit, and a plain cooldown would throw the detail away.
# Instead the first event of a burst is pushed as usual and the rest are
# collected, then delivered as ONE summary push when the window closes. At most
# three messages can leave in any window: the first event, at most one exempt
# pair-half, and the summary.
#
# The exemption matters: a subagent's started and finished are two halves of one
# task lifecycle and must both ring on the phone (subagent_state_eids pins this).
# The finish of the task we just announced is not "more of the same", so it is
# exempt -- but only once per window, so an entity can never stream through.
COALESCE_KINDS = ["subagent"]
COALESCE_WINDOW_S = 60         # collect this long, then send one summary
COALESCE_MAX_LISTED = 5        # titles named in the summary body
COALESCE_MAX_ITEMS = 100       # collect no further than this (flush instead)

# Where a workflow tends to drop screenshots on its own, relative to the
# session's project_dir (scanned in addition to the project root itself). Any
# relative path is accepted via notify.ntfy.screenshot_dirs.
DEFAULT_SCREENSHOT_DIRS = [".playwright-mcp", "docs/screenshots",
                           "screenshots", ".opencode/tmp"]
SCREENSHOT_EXTS = {".png", ".jpg", ".jpeg", ".webp"}
KIND_TAGS = {
    "commit": "arrow_right", "milestone": "tada", "escalation": "sos",
    "plan_done": "white_check_mark", "continue": "repeat", "nudge": "bell",
    "confirm_done": "check_mark_button", "permission": "lock",
    "subagent": "robot", "test": "test_tube", "routing": "chat",
    "llama": "arrows_counterclockwise",
}


def _log_path():
    return _hawk.home_dir() / "notify_log.json"


def load_notify_log() -> list:
    """Newest-last list of sent/received notification events (capped).

    Loads via retry like coordinator.load_json: _write_log swaps the file in
    with os.replace, so a concurrent reader can transiently fail on Windows
    (sharing violation) on a perfectly good file. Returning [] on such a
    transient failure would then be persisted as a wiped log by the next
    append/prune."""
    err = None
    for attempt in range(5):
        p = _log_path()
        if not p.exists():
            return []
        try:
            with open(p, "r", encoding="utf-8-sig") as f:
                d = json.load(f)
            return d if isinstance(d, list) else []
        except Exception as e:
            err = e
    log("notify: notify_log.json unreadable after 5 attempts: %s" % err)
    return []


def _write_log(entries: list) -> None:
    """Atomic rewrite of the notify log (per-pid tmp file + os.replace, so
    concurrent writers can never interleave dumps into the same tmp)."""
    tmp = _log_path().with_name("%s.%d.tmp" % (_log_path().name, os.getpid()))
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(entries, f, ensure_ascii=False)
        os.replace(tmp, _log_path())
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        raise


def append_notify_log(entry: dict) -> None:
    try:
        entries = load_notify_log()  # not "log": that would shadow log() in the except below
        entries.append(entry)
        if len(entries) > NOTIFY_LOG_CAP:
            entries = entries[-NOTIFY_LOG_CAP:]
        _write_log(entries)
    except Exception as e:
        log("notify: log append failed: %s" % e)


def remove_log_entries(predicate) -> int:
    """Drop log entries for which predicate(entry) is true; returns the
    number removed. Used to mirror remote ntfy deletes in the local list.
    Never rewrites the file when nothing matched, so a transient read
    failure cannot be persisted as a wiped log."""
    removed = 0
    try:
        entries = load_notify_log()
        kept = []
        for e in entries:
            if predicate(e):
                removed += 1
            else:
                kept.append(e)
        if not removed:
            return 0
        _write_log(kept)
    except Exception as e:
        log("notify: log prune failed: %s" % e)
    return removed


def send_test(cfg) -> bool:
    """Push a test alert (logged as dir=sent, kind=test)."""
    n = _ntfy_cfg(cfg)
    if not n["topic"]:
        return False
    body = "This is a test push from the Hawk dashboard."
    body += "\nSubscribe: %s/%s" % (n["server"], n["topic"])
    eid = "test-%d" % int(_time.time() * 1000)
    return record_event(cfg, "test", "test notification", body, eid=eid)


def _map_path():
    return _hawk.home_dir() / "pending_replies.json"


def _load_map() -> dict:
    try:
        p = _map_path()
        if p.exists():
            with open(p, "r", encoding="utf-8-sig") as f:
                d = json.load(f)
            return d if isinstance(d, dict) else {}
    except (OSError, ValueError) as e:
        log_debug("notify: unreadable route map: %s" % e)
    return {}


def _save_map(d: dict) -> None:
    try:
        with open(_map_path(), "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False)
    except Exception as e:
        log("notify: reply map save failed: %s" % e)


def _ntfy_cfg(cfg) -> dict:
    n = (cfg.get("notify") or {}).get("ntfy") or {}
    return {
        "topic": str(n.get("topic") or "").strip(),
        "server": str(n.get("server") or "https://ntfy.sh").rstrip("/"),
        "priority": int(n.get("priority") or 3),
        "callback_base": str(n.get("callback_base") or "http://127.0.0.1:8765").rstrip("/"),
    }


# ---- ntfy reachability (surfaced in the web UI) ---------------------------
# A push or reply poll that fails (connection refused / timeout / 429 / a
# blackholed IP) is recorded so the dashboard can warn while the phone has
# silently stopped receiving notifications. The two continuous paths
# (_post_ntfy, poll_ntfy_replies) are the heartbeat: whatever they last saw is
# the signal. No extra network I/O happens here, so the dashboard request path
# stays free of probes.
NTFY_HEALTH_TTL_S = 90      # a recorded exchange older than this is "stale"


def _health_path():
    return _hawk.home_dir() / "ntfy_health.json"


def read_ntfy_health() -> dict:
    """Outcome of the last recorded ntfy exchange, or {} when none/unreadable."""
    try:
        with open(_health_path(), "r", encoding="utf-8-sig") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def record_ntfy_exchange(ok: bool, error: str = "") -> None:
    """Remember whether the last real ntfy send/poll succeeded (best-effort).

    Also counts consecutive failures, so the UI can show how long ntfy has been
    down rather than only "down right now".
    Never raises: a health write must not be able to break delivery."""
    now_ms = int(_time.time() * 1000)
    prev = read_ntfy_health()
    rec = {
        "ok": bool(ok),
        "ts": now_ms,
        "error": "" if ok else str(error)[:200],
        "last_ok_ms": now_ms if ok else int(prev.get("last_ok_ms") or 0),
        "fails": 0 if ok else int(prev.get("fails") or 0) + 1,
    }
    try:
        p = _health_path()
        # Per-pid tmp + os.replace, like _write_log: a reader can never see a
        # half-written file.
        tmp = p.with_name("%s.%d.tmp" % (p.name, os.getpid()))
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False)
        os.replace(tmp, p)
    except Exception:
        pass


def ntfy_health(cfg) -> dict:
    """Reachability of the ntfy server for the web UI warning banner, derived
    from the last real send/poll outcome.

    Returns {"configured", "server", "reachable", "source", "since_ms",
    "last_ok_ms", "error", "stale", "fails"}. reachable is False only after an
    observed failure, so a fresh install (no record yet) never raises a false
    alarm; "fails" is the number of consecutive failed exchanges (0 when ok).
    Never raises and never touches the network."""
    n = _ntfy_cfg(cfg)
    out = {"configured": bool(n["topic"]), "server": n["server"],
           "reachable": True, "source": "none", "since_ms": 0,
           "last_ok_ms": 0, "error": "", "stale": False, "fails": 0}
    if not n["topic"]:
        return out
    rec = read_ntfy_health()
    if not rec:
        return out
    out["source"] = "exchange"
    out["last_ok_ms"] = int(rec.get("last_ok_ms") or 0)
    out["fails"] = int(rec.get("fails") or 0)
    out["stale"] = (int(_time.time() * 1000) - int(rec.get("ts") or 0)
                    >= NTFY_HEALTH_TTL_S * 1000)
    if rec.get("ok"):
        return out
    out["reachable"] = False
    out["error"] = rec.get("error") or "ntfy unreachable"
    out["since_ms"] = int(rec.get("ts") or 0)
    return out


def _retry_after_s(e) -> int | None:
    """Seconds ntfy asked us to wait, from a 429/503's Retry-After header.

    Only honored for the status codes that carry it; a bare HTTP-date value
    (the other legal form) is ignored rather than parsed, since the schedule in
    _poll_gap_ms already backs off and an unparseable header must never raise."""
    if getattr(e, "code", None) not in (429, 503):
        return None
    try:
        hdr = e.headers.get("Retry-After") if e.headers else None
    except Exception:
        hdr = None
    if not hdr:
        return None
    try:
        v = int(str(hdr).strip())
    except ValueError:
        return None
    return max(0, min(v, NTFY_POLL_RETRY_AFTER_CAP_S))


def _poll_gap_ms(fails: int, retry_after=None) -> int:
    """Milliseconds to wait before the next reply poll.

    Normal spacing is NTFY_POLL_MIN_GAP_S; each consecutive failure widens it
    along NTFY_POLL_BACKOFF_S (capped, so replies resume quickly once ntfy is
    back). An explicit Retry-After never shortens the scheduled gap, and a
    pathological header can't park polling for hours."""
    base = NTFY_POLL_BACKOFF_S[min(max(fails - 1, 0),
                                   len(NTFY_POLL_BACKOFF_S) - 1)] * 1000
    if retry_after:
        base = max(base, min(int(retry_after), NTFY_POLL_RETRY_AFTER_CAP_S) * 1000)
    return max(base, NTFY_POLL_MIN_GAP_S * 1000)


def _note_poll_failure(m: dict, retry_after=None) -> int:
    """Record a poll failure in the reply map and set the next gap.

    The map (not the health file) is the source of truth for pacing: it is the
    state the throttle already lives in, and it is per-poll rather than shared
    with the send path."""
    fails = int(m.get("_ntfy_fails") or 0) + 1
    m["_ntfy_fails"] = fails
    m["_ntfy_gap_ms"] = _poll_gap_ms(fails, retry_after)
    return fails


def kinds_enabled(cfg) -> list:
    """The set of kinds the phone push is gated on.

    Config load only merges top-level defaults, so an absent
    notify.ntfy.kinds key falls back to DEFAULT_NOTIFY_KINDS at read time.
    An explicit list (even empty) is respected as-is.
    """
    n = (cfg.get("notify") or {}).get("ntfy") or {}
    k = n.get("kinds")
    if isinstance(k, list):
        return [str(x) for x in k]
    return list(DEFAULT_NOTIFY_KINDS)


def cooldown_s(cfg, kind: str) -> int:
    n = (cfg.get("notify") or {}).get("ntfy") or {}
    cd = n.get("kind_cooldown_s")
    # Only an explicit entry for *this* kind overrides the default. Reading the
    # dict with a 0 default meant that configuring any cooldown (say, "commit")
    # silently gave every other kind a cooldown of 0 and threw away the
    # documented fallback -- which is how subagent completions ended up
    # unthrottled and produced the push burst that drew ntfy's 429s. An
    # explicit 0 is still honoured: that is how a kind opts out.
    if isinstance(cd, dict) and kind in cd:
        try:
            v = int(cd.get(kind))
            if v >= 0:
                return v
        except (TypeError, ValueError):
            pass
    return int(DEFAULT_KIND_COOLDOWNS_S.get(kind, 0))


def _push_state_path():
    return _hawk.home_dir() / "event_push_state.json"


def load_push_state() -> dict:
    try:
        p = _push_state_path()
        if p.exists():
            with open(p, "r", encoding="utf-8-sig") as f:
                d = json.load(f)
            return d if isinstance(d, dict) else {}
    except (OSError, ValueError) as e:
        log_debug("notify: unreadable push state: %s" % e)
    return {}


def _save_push_state(st: dict) -> None:
    try:
        eids = st.get("pushed_eids") or []
        st["pushed_eids"] = list(eids)[-PUSHED_EIDS_CAP:]
        _hawk.save_json(_push_state_path(), st)
    except Exception as e:
        log("notify: push state save failed: %s" % e)


def get_watermark(project_dir: str, kind: str):
    """Last value already pushed for (project, kind); None if never pushed.

    Lets the dashboard recompute "what is new" after a restart without
    re-pushing what it already sent.
    """
    try:
        wm = (load_push_state().get("watermarks") or {})
        return (wm.get(project_dir) or {}).get(kind)
    except Exception:
        return None


def set_watermark(project_dir: str, kind: str, value) -> None:
    try:
        st = load_push_state()
        wm = st.setdefault("watermarks", {})
        wm.setdefault(str(project_dir), {})[kind] = value
        _save_push_state(st)
    except Exception as e:
        log("notify: watermark save failed: %s" % e)


# ---- burst coalescing state ------------------------------------------------
# Kept beside the other push state on disk so a window that outlives its
# process is delivered by the next event instead of vanishing with it.
_cohort_lock = threading.Lock()
_cohort = None          # built by _cohort_empty(); replaced, never mutated
_cohort_loaded = False  # True once the state file has been read this process
_cohort_timer = None    # pending threading.Timer for the open window


def _cohort_path():
    return _hawk.home_dir() / "cohort_push.json"


def _cohort_empty() -> dict:
    return {"kind": None, "since": 0.0, "items": [],
            "last_eid": "", "last_entity": "", "exempt_used": False}


def _cohort_entity(eid) -> str:
    """The eid minus its trailing state: sub:x:started / sub:x:finished -> sub:x.

    Empty when the eid carries no state at all, which turns the pair exemption
    off rather than guessing that two unrelated events belong together."""
    eid = str(eid or "")
    return eid.rsplit(":", 1)[0] if ":" in eid else ""


def _cohort_save_locked() -> None:
    """Persist the open window. Failure is logged and swallowed: a burst
    summary is not worth raising out of the push path."""
    try:
        _hawk.save_json(_cohort_path(), _cohort)
    except Exception as e:
        log_debug("notify: cohort save failed: %s" % e)


def _cohort_ensure_loaded_locked() -> None:
    """Read cohort_push.json once per process into _cohort."""
    global _cohort, _cohort_loaded
    if _cohort_loaded:
        return
    _cohort_loaded = True
    _cohort = _cohort_empty()
    try:
        p = _cohort_path()
        if not p.exists():
            return
        with open(p, "r", encoding="utf-8-sig") as f:
            d = json.load(f)
    except (OSError, ValueError) as e:
        log_debug("notify: unreadable cohort state: %s" % e)
        return
    if not isinstance(d, dict) or not d.get("kind"):
        return
    base = _cohort_empty()
    for k in base:
        if k in d:
            base[k] = d[k]
    try:
        base["since"] = float(base.get("since") or 0.0)
    except (TypeError, ValueError):
        base["since"] = 0.0
    base["items"] = [i for i in (base.get("items") or []) if isinstance(i, dict)]
    base["last_eid"] = str(base.get("last_eid") or "")
    base["last_entity"] = str(base.get("last_entity") or "")
    base["exempt_used"] = bool(base.get("exempt_used"))
    _cohort = base


def _cohort_timer_arm_locked(delay_s: float = None) -> None:
    """(Re)arm the window timer. Must hold _cohort_lock."""
    global _cohort_timer
    if _cohort_timer is not None:
        _cohort_timer.cancel()
    t = threading.Timer(COALESCE_WINDOW_S if delay_s is None else delay_s,
                        _flush_cohort)
    t.daemon = True                  # a pending summary must not hold an exit
    _cohort_timer = t
    t.start()


def _flush_cohort() -> bool:
    """Deliver ONE summary push for whatever the open window collected.

    Runs on the window timer, and from the next record_event after a restart
    that outlived the window. Returns True only when a summary actually left."""
    global _cohort_timer, _cohort
    with _cohort_lock:
        _cohort_ensure_loaded_locked()
        items = list(_cohort.get("items") or [])
        kind = str(_cohort.get("kind") or "")
        _cohort = _cohort_empty()
        if _cohort_timer is not None:
            _cohort_timer.cancel()
            _cohort_timer = None
        _cohort_save_locked()
    if not items or not kind:
        return False
    n = len(items)
    s = "" if n == 1 else "s"
    lines = ["%d %s event%s in the last %ds:" % (n, kind, s, COALESCE_WINDOW_S)]
    for it in items[:COALESCE_MAX_LISTED]:
        t = str(it.get("title") or "")
        if t.startswith("[hawk] "):
            t = t[7:]
        lines.append("- " + t)
    rest = n - COALESCE_MAX_LISTED
    if rest > 0:
        lines.append("... and %d more (full list in the dashboard)" % rest)
    return _post_ntfy(load_config(),
                      "[hawk] %d %s event%s in the last %ds" % (n, kind, s, COALESCE_WINDOW_S),
                      "\n".join(lines), tags=KIND_TAGS.get(kind, ""),
                      actions=None, kind=kind,
                      eid="cohort:%s:%d" % (kind, int(_time.time())),
                      meta={"cohort": n}, attach="")


def _cohort_reset() -> None:
    """Drop the open window completely: cancel its timer, forget it in memory,
    and overwrite the state file so a reload cannot resurrect it.

    Nothing on the prod hot path calls this - the armed timer and _cohort_ensure
    finish a pending window instead of discarding it. Tests use it to isolate
    cases from each other."""
    global _cohort_timer, _cohort, _cohort_loaded
    with _cohort_lock:
        if _cohort_timer is not None:
            _cohort_timer.cancel()
            _cohort_timer = None
        _cohort = _cohort_empty()
        _cohort_save_locked()
        _cohort_loaded = False    # next use reads the empty file just written


def _cohort_ensure() -> None:
    """Read the persisted window once and finish it if its time has passed.

    The armed timer normally does this; the state file covers the case where
    the process died inside a window, so the next subagent event of the next
    run delivers the old burst instead of silently losing it."""
    stale = False
    with _cohort_lock:
        _cohort_ensure_loaded_locked()
        since = float(_cohort.get("since") or 0.0)
        stale = bool(_cohort.get("kind")) and (_time.time() - since) >= COALESCE_WINDOW_S
        if not stale and _cohort.get("kind") and _cohort_timer is None:
            # A restart inherited the window but not its timer: re-arm for the
            # time that is left, so the summary leaves on schedule even if no
            # further event ever arrives to notice that it is overdue.
            _cohort_timer_arm_locked(max(1.0, COALESCE_WINDOW_S - (_time.time() - since)))
    if stale:
        _flush_cohort()


def _coalesce_collect(kind: str, title: str, body: str, eid: str) -> bool:
    """True when this event rides the pending summary instead of its own push.

    The event that OPENS a window is never collected - it is pushed by the
    caller. An event for the entity just announced is exempt once per window,
    so a subagent's started and finished both ring (subagent_state_eids). Every
    other event inside the window is collected."""
    global _cohort
    entity = _cohort_entity(eid)
    now = _time.time()
    with _cohort_lock:
        _cohort_ensure_loaded_locked()
        active = (_cohort.get("kind") == kind
                  and (now - float(_cohort.get("since") or 0.0)) < COALESCE_WINDOW_S)
        exempt = (active and not bool(_cohort.get("exempt_used"))
                  and bool(entity) and entity == _cohort.get("last_entity")
                  and bool(eid) and eid != _cohort.get("last_eid"))
        if not active or exempt:
            return False
        items = _cohort.setdefault("items", [])
        if len(items) >= COALESCE_MAX_ITEMS:
            # Never collect past the cap: close the window now and let the
            # caller push this one itself, so no event is dropped.
            _cohort_save_locked()
            flush = True
        else:
            items.append({"title": title, "body": body, "eid": eid, "ts": now})
            _cohort_save_locked()
            flush = False
    if flush:
        _flush_cohort()
        return False
    return True


def _cohort_open(kind: str, eid: str) -> None:
    """Called right after an individual push of a coalescable kind.

    Opens the window the next event will be collected into, or - when a window
    is already open - records that the exempt pair-half just went out."""
    global _cohort
    entity = _cohort_entity(eid)
    now = _time.time()
    with _cohort_lock:
        _cohort_ensure_loaded_locked()
        active = (_cohort.get("kind") == kind
                  and (now - float(_cohort.get("since") or 0.0)) < COALESCE_WINDOW_S)
        if active:
            _cohort["last_eid"] = str(eid or "")
            _cohort["last_entity"] = entity
            _cohort["exempt_used"] = True    # one exemption per window, no more
        else:
            _cohort = _cohort_empty()
            _cohort["kind"] = kind
            _cohort["since"] = now
            _cohort["last_eid"] = str(eid or "")
            _cohort["last_entity"] = entity
            _cohort_timer_arm_locked()
        _cohort_save_locked()


def record_event(cfg, kind: str, title: str, body: str, eid: str = "",
                 meta: dict = None, attach: str = "") -> bool:
    """Unified phone-push entry point for every event kind.

    Applies, in order: topic configured, eid dedupe, kinds_enabled gate,
    per-kind cooldown, burst coalescing (a chatty kind inside an open window
    returns True without a second HTTP POST - it ships with the summary).
    Only a push that passes every gate is sent and logged (suppressed events
    are neither logged nor pushed).
    """
    if not _ntfy_cfg(cfg)["topic"]:
        return False
    st = load_push_state()
    if eid:
        if eid in (st.get("pushed_eids") or []):
            return False
    if kind not in kinds_enabled(cfg):
        return False
    cd = cooldown_s(cfg, kind)
    if cd:
        last = int((st.get("last_push_ts") or {}).get(kind, 0))
        if int(_time.time()) - last < cd:
            return False
    if not title.startswith("[hawk]"):
        title = "[hawk] %s" % title
    tags = KIND_TAGS.get(kind, "")
    if kind == "subagent" and (meta or {}).get("state") == "error":
        tags = "warning:" + tags   # errored subagent -> warning badge (2026-09-23)
    # Burst coalescing: inside an open window this event rides the summary, so
    # a tree that finishes 74 tasks at once costs one POST, not 74.
    if kind in COALESCE_KINDS:
        _cohort_ensure()
        if _coalesce_collect(kind, title, body, eid):
            if eid:                # remember it as handled, so a duplicate eid
                ids = list(st.get("pushed_eids") or [])   # is not re-processed
                ids.append(eid)
                st["pushed_eids"] = ids[-PUSHED_EIDS_CAP:]
                _save_push_state(st)
            return True
    sent = _post_ntfy(cfg, title, body,
                      tags=tags, actions=None,
                      kind=kind, eid=eid, meta=meta, attach=attach)
    if kind in COALESCE_KINDS:
        _cohort_open(kind, eid)    # whether or not it landed: the window counts
                                   # events, and it rate-limits retries too
    if sent:
        now = int(_time.time())
        st.setdefault("last_push_ts", {})[kind] = now
        if eid:
            ids = list(st.get("pushed_eids") or [])
            ids.append(eid)
            st["pushed_eids"] = ids[-PUSHED_EIDS_CAP:]
        _save_push_state(st)
    return sent


NTFY_MAX_BYTES = 4096  # ntfy turns a longer message into a file attachment


def fit_ntfy(body: str) -> str:
    """The whole text when it fits one ntfy message; otherwise cut at a line end with a pointer to the full text."""
    note = "\n… (full text in the Hawk dashboard)"
    if len(body.encode("utf-8")) <= NTFY_MAX_BYTES:
        return body
    room = NTFY_MAX_BYTES - len(note.encode("utf-8"))
    cut = body.encode("utf-8")[:room].decode("utf-8", errors="ignore")
    if "\n" in cut[len(cut) // 2:]:
        cut = cut[:cut.rfind("\n")]
    return cut + note


def _urlopen_retry_429(req, label: str, waits=None, timeout: int = 15):
    """urlopen with backoff-and-retry on HTTP 429.

    ntfy rate-limits per client (a burst, then ~1 request / 5 s) and hawk's own
    channel polling shares that budget, so a 429 must wait for the bucket to
    refill rather than fail the request. The 429 is re-raised once the waits
    are exhausted so each caller keeps its own error handling (send -> False,
    delete -> 404 is "gone", else False)."""
    for wait in list(NTFY_429_WAITS_S if waits is None else waits) + [None]:
        try:
            return urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            if e.code == 429 and wait is not None:
                log("notify: ntfy %s -> 429, retry in %ds" % (label, wait))
                _time.sleep(wait)
                continue
            raise


def _post_ntfy(cfg, title: str, body: str, tags: str = "",
                actions=None, kind: str = "alert", eid: str = "",
                meta: dict = None, attach: str = "") -> bool:
    n = _ntfy_cfg(cfg)
    if not n["topic"]:
        return False
    # JSON publishing (POST to the server root) keeps title and tags UTF-8;
    # header publishing forced them through latin-1 ("→" arrived as "?").
    payload = {"topic": n["topic"], "title": title[:256], "message": fit_ntfy(body),
               "priority": n["priority"]}
    if tags:
        payload["tags"] = [t for t in tags.split(",") if t]
    if actions:
        payload["actions"] = actions
    if attach:
        payload["attach"] = attach
    req = urllib.request.Request(
        n["server"] + "/",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with _urlopen_retry_429(req, "send %s" % title[:40]) as resp:
            ok = 200 <= (resp.status or 200) < 300
            msg_id = ""
            if ok:
                try:
                    raw = resp.read().decode("utf-8", errors="replace")
                    msg_id = str(json.loads(raw).get("id") or "")
                except (ValueError, AttributeError) as e:
                    # Response body not the expected JSON: keep msg_id empty, but
                    # leave a breadcrumb - a broken publish must be diagnosable.
                    log_debug("notify: unparseable publish response: %s" % e)
        log("notify: pushed to ntfy topic %s (%d action(s)) id=%s"
            % (n["topic"], len(actions or []), msg_id or "?"))
        if ok:
            entry = {
                "ts": int(_time.time() * 1000),
                "dir": "sent", "kind": kind,
                "title": title[:256],
                "body": body[:8000],
                "excerpt": body[:160].replace("\n", " "),
                "topic": n["topic"],
                "id": msg_id or ("dlt-" + uuid.uuid4().hex[:12])}
            if eid:
                entry["eid"] = eid
            if attach:
                entry["attach"] = attach
            if meta:
                for k, v in meta.items():
                    if v not in (None, ""):
                        entry[k] = v
            append_notify_log(entry)
            if msg_id:
                _track_self_id(msg_id)
        record_ntfy_exchange(ok)
        return ok
    except Exception as e:
        log("notify: ntfy send failed: %s" % e)
        record_ntfy_exchange(False, str(e))
        return False


def _screenshot_cfg(cfg) -> dict:
    """Screenshot-attachment settings under notify.ntfy (read-time defaults, so
    the key may be absent from config.json entirely)."""
    n = (cfg.get("notify") or {}).get("ntfy") or {}
    dirs = n.get("screenshot_dirs")
    return {
        "enabled": bool(n.get("attach_screenshots", True)),
        "max_age_s": int(n.get("screenshot_max_age_s") or 1800),
        "max_bytes": int(n.get("screenshot_max_bytes") or 2_000_000),
        "dirs": ([str(d) for d in dirs] if isinstance(dirs, list)
                 else list(DEFAULT_SCREENSHOT_DIRS)),
    }


def find_recent_screenshots(project_dir, max_age_s: int = 1800,
                            dirs=None, max_bytes: int = 0) -> list:
    """Image files a workflow left behind, newest first (best-effort, no raise).

    Scans the project root plus the usual output dirs (non-recursive, so a huge
    tree is never walked) and keeps images modified within max_age_s and, when
    max_bytes > 0, under that size. Missing/empty project_dir yields [].
    """
    out = []
    if not str(project_dir or "").strip():
        return out
    try:
        root = Path(str(project_dir)).expanduser()
        if not root.is_dir():
            return out
        cutoff = _time.time() - max(0, int(max_age_s))
        seen = set()
        for base in [root] + [root / d for d in (dirs or [])]:
            try:
                if not base.is_dir():
                    continue
                for p in base.iterdir():
                    if not p.is_file() or p.suffix.lower() not in SCREENSHOT_EXTS:
                        continue
                    rp = str(p.resolve())
                    if rp in seen:
                        continue
                    seen.add(rp)
                    st = p.stat()
                    if st.st_size == 0 or st.st_mtime < cutoff:
                        continue
                    if max_bytes and st.st_size > max_bytes:
                        continue
                    out.append((st.st_mtime, p))
            except OSError as e:
                log_debug("notify: screenshot scan skipped %s: %s" % (base, e))
    except OSError as e:
        log_debug("notify: screenshot scan failed: %s" % e)
    out.sort(key=lambda t: t[0], reverse=True)
    return [p for _, p in out]


def upload_attachment(cfg, path) -> str:
    """Upload a local file to the ntfy topic and return its served URL.

    ntfy's JSON publish can only *reference* an attachment by URL, so a local
    screenshot must be PUT to the topic first; the publish then carries the
    returned url. Returns "" (and logs) when the file is unreadable or the
    server rejects the upload -- e.g. self-hosted ntfy without attachment
    support -- so the push still goes out as plain text.
    """
    n = _ntfy_cfg(cfg)
    if not n["topic"]:
        return ""
    path = Path(path)
    try:
        data = path.read_bytes()
    except OSError as e:
        log_debug("notify: unreadable attachment %s: %s" % (path, e))
        return ""
    req = urllib.request.Request(
        "%s/%s" % (n["server"], n["topic"]), data=data, method="PUT",
        headers={"Filename": path.name,
                 "Content-Type": "application/octet-stream"})
    try:
        with _urlopen_retry_429(req, "upload %s" % path.name[:30],
                                timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            info = json.loads(raw)
            return str((info.get("attachment") or {}).get("url") or "")
    except Exception as e:
        log("notify: attachment upload failed (%s): %s" % (path.name, e))
        return ""


def _screenshot_attach(cfg, project_dir) -> str:
    """Best-effort: upload the newest recent workflow screenshot for
    project_dir and return its ntfy URL ("" when disabled/none/failed)."""
    sc = _screenshot_cfg(cfg)
    if not sc["enabled"] or not str(project_dir or "").strip():
        return ""
    try:
        shots = find_recent_screenshots(project_dir, sc["max_age_s"],
                                        sc["dirs"], sc["max_bytes"])
        if not shots:
            return ""
        url = upload_attachment(cfg, shots[0])
        if url:
            log("notify: attached screenshot %s" % shots[0].name)
        return url
    except Exception as e:
        log_debug("notify: screenshot attach failed: %s" % e)
        return ""


def delete_message(cfg, msg_id: str) -> bool:
    """DELETE a single ntfy message by its publish id. Idempotent server-side
    (ntfy returns 200 even for unknown or already-deleted ids); a 404 means
    the message is already gone, which is the desired end state."""
    n = _ntfy_cfg(cfg)
    if not n["topic"]:
        raise ValueError("ntfy not configured")
    url = "%s/%s/%s" % (n["server"], n["topic"], quote(str(msg_id), safe=""))
    req = urllib.request.Request(url, method="DELETE")
    try:
        with _urlopen_retry_429(req, "delete %s" % msg_id) as resp:
            ok = 200 <= (resp.status or 200) < 300
        log("notify: ntfy delete %s -> %s" % (msg_id, "ok" if ok else "failed"))
        return ok
    except urllib.error.HTTPError as e:
        log("notify: ntfy delete %s -> %s" % (msg_id, "gone" if e.code == 404 else e))
        return e.code == 404
    except Exception as e:
        log("notify: ntfy delete %s -> %s" % (msg_id, e))
        return False


def _ntfy_raw_history(cfg) -> list:
    """All events in ntfy's pollable history for the topic (SSE poll=1,
    since=all): message rows AND message_delete tombstones AND any other
    event kinds, in feed order. Shared by _ntfy_cached_events (messages
    only) and ntfy_channel_state (which also folds tombstones in)."""
    n = _ntfy_cfg(cfg)
    if not n["topic"]:
        return []
    url = "%s/%s/json?poll=1&since=all" % (n["server"], n["topic"])
    req = urllib.request.Request(url)
    events = []
    with urllib.request.urlopen(req, timeout=15) as resp:
        for raw in resp:
            line = raw.decode("utf-8", errors="replace").strip()
            if not line or line.startswith(":"):
                continue  # empty / SSE keepalive rows
            try:
                ev = json.loads(line)
            except ValueError:
                continue  # non-JSON row on the stream
            events.append(ev)
    return events


def _ntfy_cached_events(cfg) -> list:
    """Stream message events still cached on the ntfy topic (SSE poll=1).

    Returns a list of event dicts (each with at least "id" and "message").
    Shared by delete_all (id enumeration) and backfill_bodies (full bodies
    for log entries that predate the body field)."""
    return [e for e in _ntfy_raw_history(cfg)
            if (e.get("event") or "") == "message"]


_TAG_KINDS = {v: k for k, v in KIND_TAGS.items()}


def _kind_from_event(ev) -> str:
    """Event kind of a channel message: hawk pushes carry their kind's tag
    (KIND_TAGS); anything not titled "[hawk]" was typed by the user."""
    if not str(ev.get("title") or "").startswith("[hawk]"):
        return "reply"
    for t in ev.get("tags") or []:
        if t in _TAG_KINDS:
            return _TAG_KINDS[t]
    return "alert"


def ntfy_channel_state(cfg, events=None) -> list:
    """What an ntfy client shows for the topic right now, newest first, each
    as {"id", "ts" (ms), "title", "message", "kind", "tags", "priority",
    "read"}.

    ntfy's history is append-only: updates, clears and deletes are new
    events carrying the target's sequence_id (= its message id unless one
    was set on publish), and clients (web app, Android) apply them in feed
    order. We do the same: a later message with the same sequence id
    replaces the earlier one, message_clear marks it read, message_delete
    removes it. `events` is the raw feed (for tests); when omitted the live
    feed is fetched. Raises on fetch failure; callers guard."""
    n = _ntfy_cfg(cfg)
    if not n["topic"]:
        return []
    if events is None:
        events = _ntfy_raw_history(cfg)
    by_seq = {}
    for ev in events:
        evt = ev.get("event") or ""
        # A message's sequence defaults to its own id; delete/clear events
        # always name their target's sequence_id (their own id is unrelated).
        seq = str(ev.get("sequence_id")
                  or (ev.get("id") if evt == "message" else "") or "")
        if not seq:
            continue
        if evt == "message":
            by_seq[seq] = {"id": str(ev.get("id") or seq),
                           "seq": seq,
                           "ts": (ev.get("time") or 0) * 1000,
                           "title": ev.get("title") or "",
                           "message": ev.get("message") or "",
                           "kind": _kind_from_event(ev),
                           "tags": list(ev.get("tags") or []),
                           "priority": int(ev.get("priority") or 3),
                           "read": False}
        elif evt == "message_delete":
            by_seq.pop(seq, None)
        elif evt == "message_clear" and seq in by_seq:
            by_seq[seq]["read"] = True
    out = sorted(by_seq.values(), key=lambda m: m["ts"], reverse=True)
    return out


NTFY_CACHE_S = 12 * 3600  # ntfy.sh keeps a message this long; older ones are gone without a delete
HIDDEN_FN = "ntfy_hidden.json"
_bg = {"queue": [], "thread": None}
_bg_lock = threading.Lock()


def hidden_ids() -> set:
    """Channel messages cleared in the dashboard whose server delete may still be pending: hidden from the
    list until ntfy drops them (12 h)."""
    data = _hawk.load_json(_hawk.home_dir() / HIDDEN_FN, {}) or {}
    now = _time.time()
    return {k for k, t in data.items() if now - float(t or 0) < NTFY_CACHE_S}


def _hide(seqs) -> None:
    path = _hawk.home_dir() / HIDDEN_FN
    data = _hawk.load_json(path, {}) or {}
    now = _time.time()
    data = {k: t for k, t in data.items() if now - float(t or 0) < NTFY_CACHE_S}
    for s_ in seqs:
        data[str(s_)] = now
    _hawk.save_json(path, data)


def delete_once(cfg, msg_id: str):
    """One DELETE without waiting: ("ok"|"gone"|"limited"|"error", seconds to wait before retrying)."""
    n = _ntfy_cfg(cfg)
    url = "%s/%s/%s" % (n["server"], n["topic"], quote(str(msg_id), safe=""))
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="DELETE"), timeout=15) as resp:
            return ("ok" if 200 <= (resp.status or 200) < 300 else "error"), 0
    except urllib.error.HTTPError as e:
        if e.code == 429:
            try:
                wait = float(e.headers.get("Retry-After") or 5)
            except (TypeError, ValueError):
                wait = 5.0
            return "limited", max(1.0, wait)
        return ("gone" if e.code == 404 else "error"), 0
    except Exception:
        return "error", 0


def _delete_worker(cfg) -> None:
    """Server deletes in the background: as fast as ntfy allows, waiting only when it says so (429).
    Nothing waits for this; whatever it does not finish, ntfy drops after 12 h by itself."""
    done = failed = 0
    while True:
        with _bg_lock:
            if not _bg["queue"]:
                _bg["thread"] = None
                break
            mid, until = _bg["queue"][0]
        if _time.time() > until:  # expired on the server by now: nothing to delete
            with _bg_lock:
                _bg["queue"].pop(0)
            continue
        state, wait = delete_once(cfg, mid)
        if state == "limited":
            _time.sleep(wait)
            continue
        with _bg_lock:
            _bg["queue"].pop(0)
        if state in ("ok", "gone"):
            done += 1
        else:
            failed += 1
        _time.sleep(0.1)
    log("notify: background clear finished: %d deleted on the server, %d failed (ntfy drops those within 12 h)"
        % (done, failed))


def delete_all(cfg) -> dict:
    """Clear every message an ntfy client still shows, without making anyone wait.

    The ntfy app's own "clear all" only clears the phone, which is why it is instant. Deleting on the server
    takes one DELETE per message (ntfy.sh has no topic-wide delete) and is rate-limited, so: every live message
    is hidden from the dashboard at once, and the server deletes run in a background thread, only for messages
    still inside ntfy's 12 h cache. Returns {"ok", "hidden", "queued"}; raises ValueError without a topic and
    re-raises an enumeration failure."""
    n = _ntfy_cfg(cfg)
    if not n["topic"]:
        raise ValueError("ntfy not configured")
    try:
        live = ntfy_channel_state(cfg)
    except Exception as e:
        log("notify: ntfy cache enumeration failed: %s" % e)
        raise
    now = _time.time()
    seqs = list(dict.fromkeys(m["seq"] for m in live))
    _hide(seqs)
    young = [(m["seq"], m["ts"] / 1000.0 + NTFY_CACHE_S) for m in live if now - m["ts"] / 1000.0 < NTFY_CACHE_S]
    with _bg_lock:
        queued = {q for q, _ in _bg["queue"]}
        _bg["queue"].extend(item for item in dict(young).items() if item[0] not in queued)
        start = _bg["thread"] is None and bool(_bg["queue"])
        if start:
            _bg["thread"] = threading.Thread(target=_delete_worker, args=(cfg,), daemon=True, name="ntfy-clear")
            _bg["thread"].start()
    log("notify: clear-all hid %d message(s); %d server delete(s) run in the background" % (len(seqs), len(young)))
    return {"ok": True, "hidden": len(seqs), "queued": len(young)}


def backfill_bodies(cfg) -> dict:
    """Recover full message bodies for sent log entries that predate the
    body field by matching their publish id against the ntfy server cache
    (entries pushed since Q5 already carry body; received rows, dlt-
    fallback ids and already-complete entries are skipped).

    Mutates and re-writes the log only when at least one entry matched.
    Returns {"matched", "skipped", "remaining"} for the dashboard."""
    if not _ntfy_cfg(cfg)["topic"]:
        return {"matched": 0, "skipped": 0, "remaining": 0}
    entries = load_notify_log()
    targets = [e for e in entries
               if e.get("dir") == "sent" and not e.get("body")
               and e.get("id") and not str(e["id"]).startswith("dlt-")]
    if not targets:
        return {"matched": 0, "skipped": 0, "remaining": 0}
    try:
        events = _ntfy_cached_events(cfg)
    except Exception as e:
        log("notify: body backfill fetch failed: %s" % e)
        raise
    by_id = {str(ev.get("id")): str(ev.get("message") or "")
             for ev in events if ev.get("id")}
    matched = 0
    for e in targets:
        body = by_id.get(str(e["id"]))
        if not body:
            continue
        e["body"] = body
        e["backfilled"] = True
        matched += 1
    if matched:
        _write_log(entries)
    remaining = sum(1 for e in entries
                    if e.get("dir") == "sent" and not e.get("body"))
    return {"matched": matched, "skipped": len(targets) - matched,
            "remaining": remaining}


def _track_self_id(msg_id: str) -> None:
    """Record a self-published message ID so poll_ntfy_replies can skip it."""
    try:
        m = _load_map()
        ids = [str(x) for x in (m.get("_ntfy_self") or [])]
        ids.append(msg_id)
        m["_ntfy_self"] = ids[-100:]  # cap at 100
        _save_map(m)
    except Exception as e:
        log_debug("notify: could not record self-sent id: %s" % e)


def notify_escalation(cfg, session, reason, rule_hits, analysis, es_path,
                      kind: str = "escalation", project_dir: str = "") -> bool:
    """Send an escalation/question alert to the configured ntfy topic.

    When the workflow produced a screenshot (project_dir + recent image), it is
    attached so the user sees what the agent sees. Returns True if pushed.
    """
    ntfy_topic = _ntfy_cfg(cfg)["topic"]
    if not ntfy_topic:
        return False  # no channel configured — silent no-op

    sid = session.get("id", "")
    title = session.get("title", "") or ""
    last = analysis.get("last_assistant") or ""
    options = []
    try:
        options = extract_choices(last)
    except Exception as e:
        log_debug("notify: choice extraction failed: %s" % e)

    plain = "Reason: %s\n" % reason
    plain += "Session: %s\n" % (title[:60] or sid[-8:])
    hits = ", ".join(rule_hits) if rule_hits else "(none)"
    plain += "Rules: %s\n" % hits[:180]
    plain += "Report: %s\n" % es_path
    if options:
        plain += "Options: %s\n" % ", ".join(o[:40] for o in options)
    if len(last) > 60:
        plain += "\nLast message:\n%s" % last[:2500]

    # A screenshot the workflow took on its own makes the alert actionable
    # without opening the dashboard; attachment is best-effort.
    attach = _screenshot_attach(cfg, project_dir)
    if attach:
        plain += "Screenshot: attached\n"

    # ntfy: plain text notification (no action buttons). The user replies by
    # sending a message in the ntfy app on this topic; the monitor polls the
    # JSON stream and routes the reply into this session (asking which one when
    # several are active).
    reply_hint = ("\n\nReply in the ntfy app on this topic; "
                  "your reply is written into this session (if several are "
                  "active you'll be asked which one).")
    sent = record_event(
        cfg, kind, "%s: %s" % (kind, title[:60] or sid[-8:]),
        plain + reply_hint, eid="esc-%s-%d" % (sid, int(_time.time() * 1000)),
        meta={"session": str(sid)[-8:], "title": title[:60]}, attach=attach)
    if sent:
        # Register this session so the next user reply is routed to it.
        # Drop stale registrations first: a session disabled since its own
        # escalation must not sit ahead of this fresh one in the awaiting
        # FIFO and intercept the reply (see _live_awaiting).
        try:
            m = _load_map()
            awaiting = _live_awaiting(cfg, list(m.get("_ntfy_awaiting") or []))
            if sid not in awaiting:
                awaiting.append(sid)
            m["_ntfy_awaiting"] = awaiting
            _save_map(m)
        except Exception as e:
            log_debug("notify: could not mark session awaiting a reply: %s" % e)
    return sent


def send_text(cfg, text: str, kind: str = "plan_done",
              eid: str = "", meta: dict = None, project_dir: str = "") -> bool:
    """Plain alert to the ntfy topic (used for build-plan done). Attaches the
    newest recent project screenshot when one exists and the config allows."""
    return record_event(cfg, kind,
                        text.splitlines()[0] if text else "alert",
                        text, eid=eid, meta=meta,
                        attach=_screenshot_attach(cfg, project_dir))


def queue_reply(session_id: str, choice: str) -> bool:
    """Queue a mobile button choice for the monitor to deliver into the
    session (called by the dashboard's /api/notify/reply endpoint)."""
    m = _load_map()
    m["ntfy_" + uuid.uuid4().hex] = {
        "session_id": session_id, "choice": choice,
        "ts": int(_time.time() * 1000)}
    _save_map(m)
    append_notify_log({
        "ts": int(_time.time() * 1000), "dir": "received", "kind": "reply",
        "session": str(session_id)[-8:], "choice": str(choice)[:120]})
    return True


def drain_replies(apply_reply) -> int:
    """Deliver queued mobile-button replies to apply_reply(sid, choice)."""
    m = _load_map()
    done = 0
    for key in [k for k in m if k.startswith("ntfy_")]:
        entry = m[key]
        try:
            apply_reply(entry.get("session_id"), entry.get("choice"))
            m.pop(key, None)
            done += 1
        except Exception as e:
            log("notify: draining reply failed: %s" % e)
            break
    if done:
        _save_map(m)
    return done


def _pending_path():
    return _hawk.home_dir() / "pending_ntfy_routes.json"


def load_pending_route() -> dict:
    """The one pending reply-routing prompt, or {} when none is active."""
    try:
        p = _pending_path()
        if p.exists():
            with open(p, "r", encoding="utf-8-sig") as f:
                d = json.load(f)
            if isinstance(d, dict) and d.get("ts"):
                return d
    except (OSError, ValueError) as e:
        log_debug("notify: unreadable pending route: %s" % e)
    return {}


def save_pending_route(rec: dict) -> None:
    try:
        _hawk.save_json(_pending_path(), rec)
    except Exception as e:
        log("notify: pending route save failed: %s" % e)


def clear_pending_route() -> None:
    try:
        p = _pending_path()
        if p.exists():
            p.unlink()
    except Exception as e:
        log("notify: pending route clear failed: %s" % e)


def resolve_pending_route(cfg, index: int) -> dict:
    """Resolve an active numbered routing prompt (also used by the dashboard
    POST /api/notify/route). Reuses the queued-reply plumbing: the original
    message text is queued for the chosen session and the monitor's
    drain_replies delivers it. Returns {ok, ...}."""
    p = load_pending_route()
    cands = [c for c in (p.get("candidates") or []) if isinstance(c, dict)]
    if not p:
        return {"ok": False, "error": "no pending routing prompt"}
    if not (1 <= index <= len(cands)):
        return {"ok": False, "error": "route index out of range"}
    cand = cands[index - 1]
    sid = str(cand.get("sid") or "")
    text = str(p.get("text") or "")
    if not sid:
        return {"ok": False, "error": "candidate missing session id"}
    # Re-read before writing: the monitor may have resolved it concurrently.
    if load_pending_route().get("ts") != p.get("ts"):
        return {"ok": False, "error": "pending prompt changed; re-check"}
    if queue_reply(sid, text):
        clear_pending_route()
        return {"ok": True, "session": sid}
    return {"ok": False, "error": "failed to queue reply"}


def _active_session_cands(cfg) -> list:
    """Enabled monitored sessions (config order) eligible for free-reply
    routing: [{sid, project_dir}, ...]."""
    out = []
    mon = cfg.get("monitored_sessions") or {}
    if isinstance(mon, dict):
        for sid, entry in mon.items():
            if isinstance(entry, dict) and entry.get("enabled", True) \
                    and str(sid).strip():
                out.append({"sid": str(sid),
                            "project_dir": str(entry.get("project_dir") or "")})
    return out


def _candidate_label(cfg, sid) -> str:
    """Human label for a routing candidate: the config title, else the project
    folder name, else "session" (the sid suffix is always shown alongside)."""
    mon = cfg.get("monitored_sessions") or {}
    entry = mon.get(sid) if isinstance(mon, dict) else None
    entry = entry if isinstance(entry, dict) else {}
    title = str(entry.get("title") or "").strip()
    if title:
        return title
    pd = str(entry.get("project_dir") or "").replace("\\", "/").rstrip("/")
    base = pd.rsplit("/", 1)[-1] if pd else ""
    return base or "session"


def _routing_cands(cfg, awaiting) -> list:
    """Reply-target candidates, most specific first.

    The sessions awaiting an answer (the escalations the user has actually
    seen) come first; with none, every enabled monitored session. Each is
    {sid, project_dir, label, awaiting}. Several candidates make a reply
    ambiguous, so the caller asks instead of guessing."""
    mon = cfg.get("monitored_sessions") or {}
    if not isinstance(mon, dict):
        mon = {}
    out = []
    seen = set()
    for sid in awaiting:
        sid = str(sid)
        if sid in seen:
            continue
        seen.add(sid)
        entry = mon.get(sid) if isinstance(mon.get(sid), dict) else {}
        out.append({"sid": sid,
                    "project_dir": str(entry.get("project_dir") or ""),
                    "label": _candidate_label(cfg, sid),
                    "awaiting": True})
    if out:
        return out
    for c in _active_session_cands(cfg):
        c = dict(c)
        c["label"] = _candidate_label(cfg, c["sid"])
        c["awaiting"] = False
        out.append(c)
    return out


def _live_awaiting(cfg, awaiting):
    """Keep awaiting entries a reply may still legitimately reach.

    An escalation registers the session it was pushed for; that registration
    must not outlive the session's steering. Entries whose session is now an
    explicitly-disabled `monitored_sessions` key are stale (the user can no
    longer be answering a message about a session hawk has stopped
    monitoring) and are dropped -- otherwise such an entry sits at the front
    of the FIFO and hijacks the reply meant for the session behind the
    message the user actually answered (seen: a reply to a session's
    confirm_done being injected 1:1 into an unrelated, disabled session).
    Sids with no config entry at all are kept: legacy single-session targets
    and subagents never carry a `monitored_sessions` row."""
    mon = cfg.get("monitored_sessions") or {}
    if not isinstance(mon, dict):
        mon = {}
    live = {c["sid"] for c in _active_session_cands(cfg)}
    return [a for a in awaiting if a in live or str(a) not in mon]


def poll_ntfy_replies(apply_reply) -> int:
    """Poll the ntfy JSON stream for user replies and route them into the
    target session via apply_reply(session_id, text).

    Targets are the awaiting escalations first, else every enabled monitored
    session; a single target is delivered directly, several push a numbered
    routing prompt. Uses poll=1 (cached messages, close connection) with
    since=<cursor> for incremental polling.  Self-published message IDs are
    tracked in the reply map so the coordinator never treats its own
    notifications as replies.  Returns how many replies were delivered.
    """
    cfg = load_config()
    n = _ntfy_cfg(cfg)
    if not n["topic"]:
        return 0
    m = _load_map()

    # Throttle: at most NTFY_POLL_MIN_GAP_S seconds between attempts, widening
    # after consecutive failures (the circuit breaker).
    #
    # The timestamp must be written to disk *before* the request. Saving only
    # after a successful parse (the old behaviour) meant every failure threw
    # the throttle away, so an unreachable ntfy was re-polled on every monitor
    # loop tick (~1/s). That sustained burst is what tripped ntfy's per-client
    # rate limit and got this machine's IP temporarily blocked, and it
    # happened again on the next ntfy hiccup.
    now_ms = int(_time.time() * 1000)
    last_polled = int(m.get("_ntfy_polled_at") or 0)
    gap_ms = int(m.get("_ntfy_gap_ms") or (NTFY_POLL_MIN_GAP_S * 1000))
    if now_ms - last_polled < gap_ms:
        return 0
    m["_ntfy_polled_at"] = now_ms
    _save_map(m)

    cursor = m.get("_ntfy_cursor") or ""
    if not cursor:
        # First run: fetch the entire cache, then set cursor to the latest
        # message ID so subsequent polls only see new messages.
        cursor = "all"

    self_ids = set(str(x) for x in (m.get("_ntfy_self") or []))
    seen = set(str(x) for x in (m.get("_ntfy_seen") or []))
    awaiting = list(m.get("_ntfy_awaiting") or [])

    # A reply answers the message the user just saw. Escalations register the
    # session they were pushed for; registrations for sessions the user has
    # since disabled are stale and must not intercept the reply (a stale
    # entry ahead of a fresh one once routed a confirm_done reply into an
    # unrelated, disabled session).
    awaiting = _live_awaiting(cfg, awaiting)

    url = "%s/%s/json?poll=1&since=%s" % (n["server"], n["topic"], cursor)
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        # No in-call retry loop here on purpose: _post_ntfy may block this
        # thread for up to 38 s on 429s, but the reply poll runs inside the
        # monitor's main loop and must stay responsive. Pacing plus the
        # Retry-After honouring below does the same job across polls instead.
        log("notify: ntfy reply poll failed: %s" % e)
        record_ntfy_exchange(False, str(e))
        _note_poll_failure(m, _retry_after_s(e))
        _save_map(m)
        return 0

    record_ntfy_exchange(True)
    # Recovered: full speed again from the very next poll.
    m["_ntfy_fails"] = 0
    m["_ntfy_gap_ms"] = NTFY_POLL_MIN_GAP_S * 1000

    delivered = 0
    new_cursor = cursor
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        event = ev.get("event") or ""
        mid = ev.get("id") or ""
        if not mid:
            continue
        if event != "message":
            # keepalive / open / close / poll_request: just advance cursor
            new_cursor = mid
            continue
        # Skip our own notifications: every hawk push is titled "[hawk] …", so
        # a message whose title carries that marker is a self-published
        # notification (subagent started/finished, milestone, plan_done, …),
        # never a user reply. Robust even when ntfy returned no publish id
        # (so _track_self_id never ran) and the id-based self set can't match
        # -- that path is what echoed "finished" pushes back into the session
        # as if they were user messages.
        if str(ev.get("title") or "").strip().startswith("[hawk]"):
            new_cursor = mid
            seen.add(mid)
            continue
        # Already processed or published by us?
        if mid in seen or mid in self_ids:
            new_cursor = mid
            seen.add(mid)
            continue
        body = ev.get("message") or ""
        if not body:
            new_cursor = mid
            seen.add(mid)
            continue
        new_cursor = mid
        seen.add(mid)
        body = str(body).strip()

        # 1) A pending numbered routing prompt is answered first: a digit in
        #    range resolves it (the original text is delivered to the chosen
        #    session). Awaiting registrations already name their target, so the
        #    prompt is only consulted for a reply that has no awaiting target.
        if not awaiting:
            p = load_pending_route()
            if p:
                pcands = [c for c in (p.get("candidates") or [])
                          if isinstance(c, dict)]
                if body.isdigit() and 1 <= int(body) <= len(pcands):
                    sid = str(pcands[int(body) - 1].get("sid") or "")
                    orig = str(p.get("text") or "")
                    if sid and load_pending_route().get("ts") == p.get("ts"):
                        try:
                            apply_reply(sid, orig)
                            delivered += 1
                            clear_pending_route()
                            append_notify_log({
                                "ts": int(_time.time() * 1000),
                                "dir": "received", "kind": "reply",
                                "channel": "ntfy",
                                "session": str(sid)[-8:],
                                "text": orig[:120],
                                "note": "routed via prompt #%d"
                                        % int(body)})
                        except Exception as e:
                            log("notify: ntfy reply apply failed: %s" % e)
                            # leave the prompt in place for the next tick
                        continue
                # Anything else supersedes the old prompt (A3): the incoming
                # message becomes the payload of the fresh decision below.
                append_notify_log({
                    "ts": int(_time.time() * 1000), "dir": "received",
                    "kind": "reply_expired", "channel": "ntfy",
                    "text": str(p.get("text") or "")[:120]})
                clear_pending_route()

        # 2) Reply targets: the sessions awaiting an answer (the escalations
        #    the user has seen) first, else every enabled monitored session.
        #    Exactly one target is unambiguous; with several, the user must
        #    choose, so concurrent sessions never silently steal a reply.
        cands = _routing_cands(cfg, awaiting)
        if len(cands) == 1:
            sid = cands[0]["sid"]
            if sid in awaiting:
                awaiting.remove(sid)      # the escalation was just answered
            try:
                if apply_reply(sid, body) is False:
                    raise RuntimeError("apply_reply returned False")
                delivered += 1
                append_notify_log({
                    "ts": int(_time.time() * 1000), "dir": "received",
                    "kind": "reply", "channel": "ntfy",
                    "session": str(sid)[-8:], "text": body[:120]})
            except Exception as e:
                log("notify: ntfy reply apply failed: %s" % e)
                if sid not in awaiting:
                    awaiting.append(sid)   # retry on the next tick
            continue

        # 3) Nothing to route into.
        if not cands:
            append_notify_log({
                "ts": int(_time.time() * 1000), "dir": "received",
                "kind": "reply_unrouted", "channel": "ntfy",
                "text": body[:120]})
            continue

        # 4) Several targets: ask. The prompt now owns the choice, so the
        #    awaiting FIFO is cleared -- a later reply resolves via the prompt
        #    instead of re-prompting from a stale registration.
        rec = {"ts": int(_time.time() * 1000), "text": body,
               "candidates": cands}
        save_pending_route(rec)
        awaiting = []
        prompt = ("Reply routing — %d sessions could receive this. "
                  "Send the number of the target:\n" % len(cands))
        for i, c in enumerate(cands, 1):
            prompt += "%d. %s [%s]\n" % (i, c.get("label") or "session",
                                         str(c.get("sid") or "")[-8:])
        _post_ntfy(cfg, "[hawk] reply routing", prompt,
                   tags="chat", kind="routing")

    m["_ntfy_cursor"] = new_cursor
    m["_ntfy_awaiting"] = awaiting
    # Cap the seen set to prevent unbounded growth
    if len(seen) > 200:
        seen = set(list(seen)[-200:])
    m["_ntfy_seen"] = list(seen)
    _save_map(m)
    return delivered