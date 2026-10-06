"""Phone notifications for hawk coordinator escalations/questions.

Stdlib-only (urllib + uuid + json). Channel:

- ntfy: notify.ntfy.topic -> push to https://ntfy.sh/<topic> as plain text
  (no action buttons). The user replies by sending a message in the ntfy app
  on the same topic; the monitor polls the topic's JSON stream endpoint
  (poll=1&since=<cursor>) and routes each user reply:
    1. an escalation-registered awaiting session (explicit 1:1, as before)
    2. an active numbered routing prompt (digit reply resolves it)
    3. a single enabled monitored session (free messages echo into it)
    4. otherwise: a numbered ntfy prompt is pushed and the reply waits in
       pending_ntfy_routes.json (one at a time; superseded by newer messages)
  Self-published message IDs are tracked so the coordinator never treats its
  own notifications as replies.

All phone pushes flow through record_event(kind, title, body, eid), which
applies the notify.ntfy.kinds gate (checkboxes; absent key falls back to the
default set), per-kind cooldowns (notify.ntfy.kind_cooldown_s) and eid
dedupe (event_push_state.json). Suppressed events are neither logged nor
pushed. The web UI always shows every event regardless of the push gate.
"""

import json
import os
import time as _time
import urllib.error
import urllib.request
import threading
import uuid
from urllib.parse import quote

from .coordinator import log, load_config, extract_choices
from . import coordinator as _hawk

NOTIFY_LOG_CAP = 500
# eid dedupe memory; must cover every event inside the dashboard's subagent
# push lookback window (2 h), across all sessions.
PUSHED_EIDS_CAP = 1000
# Back-off between DELETE retries when ntfy.sh answers 429 (seconds).
DELETE_429_WAITS_S = [6, 12, 20]

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
DEFAULT_KIND_COOLDOWNS_S = {"commit": 60, "continue": 60, "nudge": 60}
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
    except Exception:
        pass
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
    if isinstance(cd, dict):
        try:
            v = int(cd.get(kind, 0))
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
    except Exception:
        pass
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


def record_event(cfg, kind: str, title: str, body: str, eid: str = "",
                 meta: dict = None) -> bool:
    """Unified phone-push entry point for every event kind.

    Applies, in order: topic configured, eid dedupe, kinds_enabled gate,
    per-kind cooldown. Only a push that passes every gate is sent and
    logged (suppressed events are neither logged nor pushed).
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
    sent = _post_ntfy(cfg, title, body,
                      tags=tags, actions=None,
                      kind=kind, eid=eid, meta=meta)
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


def _post_ntfy(cfg, title: str, body: str, tags: str = "",
                actions=None, kind: str = "alert", eid: str = "",
                meta: dict = None) -> bool:
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
    req = urllib.request.Request(
        n["server"] + "/",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            ok = 200 <= (resp.status or 200) < 300
            msg_id = ""
            if ok:
                try:
                    raw = resp.read().decode("utf-8", errors="replace")
                    msg_id = str(json.loads(raw).get("id") or "")
                except Exception:
                    pass
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
            if meta:
                for k, v in meta.items():
                    if v not in (None, ""):
                        entry[k] = v
            append_notify_log(entry)
            if msg_id:
                _track_self_id(msg_id)
        return ok
    except Exception as e:
        log("notify: ntfy send failed: %s" % e)
        return False


def delete_message(cfg, msg_id: str) -> bool:
    """DELETE a single ntfy message by its publish id. Idempotent server-side
    (ntfy returns 200 even for unknown or already-deleted ids); a 404 means
    the message is already gone, which is the desired end state."""
    n = _ntfy_cfg(cfg)
    if not n["topic"]:
        raise ValueError("ntfy not configured")
    url = "%s/%s/%s" % (n["server"], n["topic"], quote(str(msg_id), safe=""))
    # ntfy.sh rate-limits per client (a burst, then ~1 request / 5 s) and
    # hawk's own channel polling shares that budget, so a 429 waits for the
    # bucket to refill and retries instead of failing the delete.
    for attempt, wait in enumerate(DELETE_429_WAITS_S + [None]):
        req = urllib.request.Request(url, method="DELETE")
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                ok = 200 <= (resp.status or 200) < 300
            log("notify: ntfy delete %s -> %s" % (msg_id, "ok" if ok else "failed"))
            return ok
        except urllib.error.HTTPError as e:
            if e.code == 429 and wait is not None:
                log("notify: ntfy delete %s -> 429, retry in %ds" % (msg_id, wait))
                _time.sleep(wait)
                continue
            log("notify: ntfy delete %s -> %s" % (msg_id, "gone" if e.code == 404 else e))
            return e.code == 404
        except Exception as e:
            log("notify: ntfy delete %s failed: %s" % (msg_id, e))
            return False
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
            except Exception:
                continue
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
    except Exception:
        pass


def notify_escalation(cfg, session, reason, rule_hits, analysis, es_path,
                      kind: str = "escalation") -> bool:
    """Send an escalation/question alert to the configured ntfy topic.

    Returns True if the alert was pushed.
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
    except Exception:
        pass

    plain = "Reason: %s\n" % reason
    plain += "Session: %s\n" % (title[:60] or sid[-8:])
    hits = ", ".join(rule_hits) if rule_hits else "(none)"
    plain += "Rules: %s\n" % hits[:180]
    plain += "Report: %s\n" % es_path
    if options:
        plain += "Options: %s\n" % ", ".join(o[:40] for o in options)
    if len(last) > 60:
        plain += "\nLast message:\n%s" % last[:2500]

    # ntfy: plain text notification (no action buttons). The user replies by
    # sending a message in the ntfy app on this topic; the monitor polls the
    # JSON stream and routes replies 1:1 into the originating session.
    reply_hint = ("\n\nReply in the ntfy app on this topic; "
                  "your reply is written 1:1 into the session.")
    sent = record_event(
        cfg, kind, "%s: %s" % (kind, title[:60] or sid[-8:]),
        plain + reply_hint, eid="esc-%s-%d" % (sid, int(_time.time() * 1000)),
        meta={"session": str(sid)[-8:], "title": title[:60]})
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
        except Exception:
            pass
    return sent


def send_text(cfg, text: str, kind: str = "plan_done",
              eid: str = "", meta: dict = None) -> bool:
    """Plain alert to the ntfy topic (used for build-plan done)."""
    return record_event(cfg, kind,
                        text.splitlines()[0] if text else "alert",
                        text, eid=eid, meta=meta)


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
    except Exception:
        pass
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
    """Poll the ntfy JSON stream for user replies and route them 1:1 into
    the originating session via apply_reply(session_id, text).

    Uses poll=1 (cached messages, close connection) with since=<cursor> for
    incremental polling.  Self-published message IDs are tracked in the
    reply map so the coordinator never treats its own notifications as
    replies.  Returns how many replies were delivered.
    """
    cfg = load_config()
    n = _ntfy_cfg(cfg)
    if not n["topic"]:
        return 0
    m = _load_map()

    # Throttle: at most once per 10 seconds
    now_ms = int(_time.time() * 1000)
    last_polled = int(m.get("_ntfy_polled_at") or 0)
    if now_ms - last_polled < 10_000:
        return 0
    m["_ntfy_polled_at"] = now_ms

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
        log("notify: ntfy reply poll failed: %s" % e)
        return 0

    delivered = 0
    new_cursor = cursor
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except Exception:
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

        # 1) Escalations register an explicit awaiting session; that reply
        #    wins over everything else (pre-unification behavior). The newest
        #    registration wins: the user answers the latest message pushed.
        if awaiting:
            sid = awaiting.pop()
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
                # Put the session back at the end (newest) for retry
                awaiting.append(sid)
            continue

        # 2) Active numbered routing prompt: a digit in range resolves it.
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
            # message becomes the payload of the fresh prompt below.
            append_notify_log({
                "ts": int(_time.time() * 1000), "dir": "received",
                "kind": "reply_expired", "channel": "ntfy",
                "text": str(p.get("text") or "")[:120]})
            clear_pending_route()

        # 3) Free message: echo it into a running session.
        cands = _active_session_cands(cfg)
        if len(cands) == 1:
            sid = cands[0]["sid"]
            try:
                apply_reply(sid, body)
                delivered += 1
                append_notify_log({
                    "ts": int(_time.time() * 1000), "dir": "received",
                    "kind": "reply", "channel": "ntfy",
                    "session": str(sid)[-8:], "text": body[:120]})
            except Exception as e:
                log("notify: ntfy reply apply failed: %s" % e)
        elif not cands:
            append_notify_log({
                "ts": int(_time.time() * 1000), "dir": "received",
                "kind": "reply_unrouted", "channel": "ntfy",
                "text": body[:120]})
        else:
            rec = {"ts": int(_time.time() * 1000), "text": body,
                   "candidates": cands}
            save_pending_route(rec)
            prompt = ("Reply routing — send the number of the target "
                      "session:\n")
            for i, c in enumerate(cands, 1):
                prompt += "%d. %s\n" % (i, str(c.get("sid") or "")[-8:])
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