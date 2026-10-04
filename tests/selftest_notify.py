"""Self-test for the unified ntfy event pipeline.

Covers: kinds_enabled fallback, record_event gate ordering (topic -> eid
dedupe -> kinds gate -> cooldown), watermarks, resolve_pending_route, and
every reply-routing branch in poll_ntfy_replies (awaiting, digit-resolves-
prompt, supersede, single-candidate echo, unrouted, multi-candidate prompt).

Hermetic: monkey-patches the home dir, config loader, ntfy HTTP layer,
urlopen, and the reply queue so the real home state, config.json, ntfy
service, and live sessions are never touched.

Run:  python tests/selftest_notify.py     (exit 0 = all pass)
"""
import json
import shutil
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import notify  # noqa: E402
import coordinator as hawk  # noqa: E402

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("ok   %s" % name)
    else:
        FAIL += 1
        print("FAIL %s  %s" % (name, detail))


class FakeResp:
    def __init__(self, payload=b"", status=200):
        self._payload = (payload if isinstance(payload, bytes)
                         else str(payload).encode("utf-8"))
        self.status = status

    def read(self):
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeStreamResp:
    """Iterable fake response: yields one bytes row per stream line."""
    def __init__(self, rows, status=200):
        self._rows = [r if isinstance(r, bytes) else r.encode("utf-8")
                      for r in rows]
        self.status = status

    def __iter__(self):
        return iter(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def install(cfg):
    """Point notify at a fresh temp home dir + fixed config; stub out all
    network and side-effect seams. Returns (restore_fn, pushes, queued)."""
    tmp = Path(tempfile.mkdtemp(prefix="ntfy-test-"))
    orig_home = hawk.home_dir
    hawk.home_dir = lambda: tmp
    orig_loadcfg = notify.load_config
    notify.load_config = lambda: cfg
    orig_post = notify._post_ntfy
    pushes = []

    def rec_post(c, title, body, tags="", actions=None, kind="alert",
                 eid="", meta=None):
        pushes.append({"title": title, "body": body, "kind": kind,
                       "eid": eid, "tags": tags})
        # Delegate to the real _post_ntfy (urlopen is stubbed) so the
        # log-append / self-id bookkeeping inside it actually runs.
        return orig_post(c, title, body, tags=tags, actions=actions,
                         kind=kind, eid=eid, meta=meta)

    notify._post_ntfy = rec_post
    orig_qr = notify.queue_reply
    queued = []
    notify.queue_reply = lambda sid, text: (queued.append((sid, text)), True)[1]
    orig_urlopen = urllib.request.urlopen
    urllib.request.urlopen = lambda req, timeout=None: FakeResp(
        json.dumps({"id": "t1"}))

    def restore():
        hawk.home_dir = orig_home
        notify.load_config = orig_loadcfg
        notify._post_ntfy = orig_post
        notify.queue_reply = orig_qr
        urllib.request.urlopen = orig_urlopen
        shutil.rmtree(tmp, ignore_errors=True)

    return restore, pushes, queued


def ndjson(events):
    return "\n".join(json.dumps(e) for e in events) + "\n"


def stub_urlopen(payload):
    orig = urllib.request.urlopen
    urllib.request.urlopen = lambda req, timeout=None: FakeResp(payload)
    return orig


def msg(mid, body):
    return {"event": "message", "id": mid, "message": body}


# ---------------------------------------------------------------- T1 kinds
def test_kinds_enabled():
    check("kinds: absent key -> defaults",
          notify.kinds_enabled({"notify": {"ntfy": {"topic": "t"}}})
          == notify.DEFAULT_NOTIFY_KINDS)
    check("kinds: explicit list respected",
          notify.kinds_enabled(
              {"notify": {"ntfy": {"kinds": ["commit"]}}}) == ["commit"])
    check("kinds: explicit empty list respected",
          notify.kinds_enabled(
              {"notify": {"ntfy": {"kinds": []}}}) == [])
    check("kinds: no notify key -> defaults",
          notify.kinds_enabled({}) == notify.DEFAULT_NOTIFY_KINDS)


# ------------------------------------------------------- T2 record_event
def test_record_event_gates():
    # no topic
    cfg0 = {"notify": {"ntfy": {"topic": ""}}}
    r, pushes, _ = install(cfg0)
    try:
        check("record: no topic -> False, no push",
              notify.record_event(cfg0, "commit", "t", "b") is False
              and not pushes)
    finally:
        r()

    # kinds gate
    cfg = {"notify": {"ntfy": {"topic": "t", "kinds": ["commit"]}}}
    r, pushes, _ = install(cfg)
    try:
        check("record: disabled kind suppressed, nothing logged",
              notify.record_event(cfg, "nudge", "t", "b") is False
              and not pushes and not notify.load_notify_log())
        check("record: enabled kind pushes with [hawk] prefix + log entry",
              notify.record_event(cfg, "commit", "t", "b", eid="e1") is True
              and len(pushes) == 1
              and pushes[0]["title"] == "[hawk] t"
              and pushes[0]["eid"] == "e1"
              and (lambda l: l and l[-1]["dir"] == "sent"
                   and l[-1]["kind"] == "commit"
                   and l[-1]["eid"] == "e1")(notify.load_notify_log()))
        check("record: same eid deduped (no second push)",
              notify.record_event(cfg, "commit", "t", "b", eid="e1") is False
              and len(pushes) == 1)
        check("record: log entry stores ntfy publish id",
              (lambda l: l and l[-1].get("id") == "t1")
              (notify.load_notify_log()))
    finally:
        r()

    # fallback id when the publish response carries no id (2026-09-25)
    cfg4 = {"notify": {"ntfy": {"topic": "t", "kinds": ["commit"]}}}
    r, pushes, _ = install(cfg4)
    try:
        orig = urllib.request.urlopen
        urllib.request.urlopen = lambda req, timeout=None: FakeResp(b"{}", 200)
        try:
            notify.record_event(cfg4, "commit", "t", "b", eid="e2")
        finally:
            urllib.request.urlopen = orig
        check("record: fallback dlt- id when response lacks id",
              (lambda l: l and str(l[-1].get("id") or "").startswith("dlt-"))
              (notify.load_notify_log()))
    finally:
        r()

    # subagent error events must push as warnings (design 2026-09-23): the
    # tag set gains "warning" on top of the robot kind tag, so the phone
    # renderer flashes a warning badge for errored subagents.
    cfg2 = {"notify": {"ntfy": {"topic": "t", "kinds": ["subagent"]}}}
    r, pushes, _ = install(cfg2)
    try:
        notify.record_event(cfg2, "subagent", "subagent x: y", "b -> c",
                            eid="sub:d1", meta={"state": "error"})
        check("subagent_error_tag",
              (pushes[-1].get("tags") or "").startswith("warning:")
              and "robot" in pushes[-1]["tags"])
    finally:
        r()

    # Started vs finished wall (update 2026-09-25): both ring on the phone.
    # The eid carries the state, so the finish is NEVER deduped by the start
    # of the same subagent, and a re-push of the same wall still dedupes.
    cfg3 = {"notify": {"ntfy": {"topic": "t", "kinds": ["subagent"]}}}
    r, pushes, _ = install(cfg3)
    try:
        notify.record_event(cfg3, "subagent", "subagent x: y",
                            "started\n08:00:00 -> 08:00:05",
                            eid="sub:p1:started", meta={"state": "started"})
        notify.record_event(cfg3, "subagent", "subagent x: y",
                            "finished\n08:00:00 -> 08:05:00",
                            eid="sub:p1:finished", meta={"state": "finished"})
        notify.record_event(cfg3, "subagent", "subagent x: y", "dup",
                            eid="sub:p1:started", meta={"state": "started"})
        check("subagent_state_eids",
              len(pushes) == 2
              and [p["eid"] for p in pushes] == ["sub:p1:started", "sub:p1:finished"]
              and all("robot" in (p.get("tags") or "") for p in pushes))
    finally:
        r()


# ---------------------------------------------------- T2b llama (infra) kind
def test_llama_kind():
    # The llama restart/down notices are INFRA events, not build-plan
    # completions: they get their own kind + tag + checkbox instead of being
    # mislabeled plan_done (update 2026-09-25).
    check("llama_not_default", "llama" not in notify.DEFAULT_NOTIFY_KINDS)
    cfg = {"notify": {"ntfy": {"topic": "t", "kinds": ["plan_done"]}}}
    r, pushes, _ = install(cfg)
    try:
        check("llama_kind_optin_gate",
              notify.record_event(cfg, "llama", "llama down", "x") is False
              and not pushes)
    finally:
        r()
    cfg2 = {"notify": {"ntfy": {"topic": "t", "kinds": ["llama"]}}}
    r, pushes, _ = install(cfg2)
    try:
        check("llama_kind_pushes",
              notify.record_event(cfg2, "llama", "llama down", "x",
                                  eid="ll:1", meta={"kind": "llama_down"}) is True
              and len(pushes) == 1
              and pushes[0]["kind"] == "llama"
              and pushes[0]["title"] == "[hawk] llama down"
              and pushes[0]["tags"] == "arrows_counterclockwise"
              and pushes[0]["eid"] == "ll:1")
        check("llama_kind_in_taxonomy",
              "llama" in notify.EVENT_KINDS
              and notify.KIND_TAGS.get("llama") == "arrows_counterclockwise")
    finally:
        r()


# ----------------------------------------------------------- T3 cooldown
def test_cooldown():
    cfg = {"notify": {"ntfy": {"topic": "t", "kinds": ["commit", "milestone"]}}}
    r, pushes, _ = install(cfg)
    try:
        check("cooldown: first commit push goes through",
              notify.record_event(cfg, "commit", "t", "b") is True
              and len(pushes) == 1)
        check("cooldown: commit within 60s suppressed",
              notify.record_event(cfg, "commit", "t2", "b") is False
              and len(pushes) == 1)
        check("cooldown: zero-cooldown kind unaffected",
              notify.record_event(cfg, "milestone", "t", "b") is True
              and len(pushes) == 2)
        check("cooldown: custom cooldown_s respected",
              notify.cooldown_s(
                  {"notify": {"ntfy": {"kind_cooldown_s": {"nudge": 5}}}},
                  "nudge") == 5
              and notify.cooldown_s({}, "nudge") == 60)
    finally:
        r()


# --------------------------------------------------------- T4 watermarks
def test_watermarks():
    r, _, _ = install({})
    try:
        check("watermark: unset -> None",
              notify.get_watermark("/projA", "commit") is None)
        notify.set_watermark("/projA", "commit", 42)
        check("watermark: roundtrip",
              notify.get_watermark("/projA", "commit") == 42)
        notify.set_watermark("/projA", "milestone", "v2")
        check("watermark: per-kind isolation",
              notify.get_watermark("/projA", "commit") == 42
              and notify.get_watermark("/projA", "milestone") == "v2"
              and notify.get_watermark("/projB", "commit") is None)
    finally:
        r()


# ------------------------------------------------- T5 resolve_pending_route
def test_resolve_pending():
    r, _, queued = install({})
    try:
        check("resolve: no pending -> error",
              notify.resolve_pending_route(None, 1)
              == {"ok": False, "error": "no pending routing prompt"})
        notify.save_pending_route({
            "ts": 111, "text": "orig question",
            "candidates": [{"sid": "s1", "project_dir": "/p1"},
                           {"sid": "s2", "project_dir": "/p2"}]})
        check("resolve: out-of-range index -> error",
              notify.resolve_pending_route(None, 5)["error"]
              == "route index out of range")
        res = notify.resolve_pending_route(None, 2)
        check("resolve: valid index queues original text, clears prompt",
              res == {"ok": True, "session": "s2"}
              and queued == [("s2", "orig question")]
              and notify.load_pending_route() == {})
    finally:
        r()


# --------------------------------------------- T6 poll routing branches
def test_poll_branches():
    applied = []
    def apply(sid, text):
        applied.append((sid, text))

    def run_once(payload, cfg=None, seed_map=None, pending=None,
                 apply_fn=None, out_map=None):
        applied.clear()  # fresh applied-list per subtest
        # poll early-returns without a topic, so ensure one is present.
        base = {"notify": {"ntfy": {"topic": "t"}}}
        cfg = dict(cfg) if cfg else {}
        cfg.setdefault("notify", base["notify"])
        r, pushes, _ = install(cfg)
        orig_urlopen = stub_urlopen(payload)
        if seed_map is not None:
            notify._save_map(seed_map)
        if pending is not None:
            notify.save_pending_route(pending)
        try:
            delivered = notify.poll_ntfy_replies(apply_fn or apply)
            log = notify.load_notify_log()
            pending = notify.load_pending_route()
            if out_map is not None:
                out_map.update(notify._load_map())
        finally:
            urllib.request.urlopen = orig_urlopen
            r()
        return delivered, log, pushes, pending

    # 6a awaiting wins
    d, lg, _, _ = run_once(ndjson([msg("m1", "hello")]),
                           seed_map={"_ntfy_awaiting": ["sidAAA"]})
    check("poll: awaiting session wins 1:1",
          d == 1 and applied == [("sidAAA", "hello")]
          and lg and lg[-1]["kind"] == "reply"
          and lg[-1]["session"] == "sidAAA"[-8:]
          and lg[-1]["channel"] == "ntfy")

    pend2 = {"ts": 222, "text": "orig question",
             "candidates": [{"sid": "s1", "project_dir": "/p1"},
                            {"sid": "s2", "project_dir": "/p2"}]}

    # 6b digit resolves prompt
    d, lg, _, pend = run_once(ndjson([msg("m1", "2")]),
                              pending=dict(pend2))
    check("poll: digit resolves numbered prompt with original text",
          d == 1 and applied == [("s2", "orig question")]
          and pend == {}
          and lg and lg[-1]["note"] == "routed via prompt #2"
          and lg[-1]["session"] == "s2"[-8:])

    # 6c non-digit supersedes prompt -> expired + unrouted (no cands)
    d, lg, _, pend = run_once(ndjson([msg("m1", "9")]),
                              pending=dict(pend2))
    kinds = [e["kind"] for e in lg]
    check("poll: out-of-range digit supersedes (expired + unrouted)",
          d == 0 and applied == [] and pend == {}
          and kinds == ["reply_expired", "reply_unrouted"])

    # 6d single candidate echo
    d, lg, _, _ = run_once(ndjson([msg("m1", "go")]),
                           cfg={"monitored_sessions":
                                {"sX": {"enabled": True,
                                        "project_dir": "/p"}}})
    check("poll: single candidate gets free message",
          d == 1 and applied == [("sX", "go")]
          and lg and lg[-1]["kind"] == "reply"
          and lg[-1]["session"] == "sX"[-8:])

    # 6e no candidates -> unrouted
    d, lg, _, _ = run_once(ndjson([msg("m1", "go")]))
    check("poll: no candidates -> reply_unrouted, nothing applied",
          d == 0 and applied == []
          and lg and lg[-1]["kind"] == "reply_unrouted")

    # 6f multi candidates -> fresh numbered prompt pushed + state saved
    d, lg, pushes, pend = run_once(
        ndjson([msg("m1", "go")]),
        cfg={"monitored_sessions":
             {"s1": {"enabled": True, "project_dir": "/p1"},
              "s2": {"enabled": True, "project_dir": "/p2"}}})
    check("poll: multi candidates -> prompt pushed + state saved",
          d == 0 and applied == []
          and pend.get("text") == "go"
          and [c.get("sid") for c in pend.get("candidates") or []]
              == ["s1", "s2"]
          and any(x["kind"] == "routing"
                  and x["title"] == "[hawk] reply routing"
                  and "1. s1" in x["body"] and "2. s2" in x["body"]
                  for x in pushes))

    # 6g seen ids skipped
    d, lg, _, _ = run_once(ndjson([msg("m9", "skip me"), msg("m10", "go")]),
                           seed_map={"_ntfy_seen": ["m9"]})
    check("poll: seen ids skipped, new ones processed",
          d == 0 and applied == []
          and len(lg) == 1 and lg[-1]["kind"] == "reply_unrouted"
          and lg[-1]["text"] == "go")

    # 6h self ids skipped
    d, lg, _, _ = run_once(ndjson([msg("ms", "own push")]),
                           seed_map={"_ntfy_self": ["ms"]})
    check("poll: self-published ids ignored",
          d == 0 and applied == [] and lg == [])

    # 6h-bis [hawk]-titled messages are self notifications, never replies.
    # Robust even when ntfy returned no publish id (so _track_self_id ran on
    # nothing and the id-based self set can't match) -- the path that echoed
    # "finished" pushes back into the session as user messages.
    hawk_msg = {"event": "message", "id": "m11",
                "message": "finished\n12:00 -> 12:05",
                "title": "[hawk] subagent x: y"}
    d, lg, _, _ = run_once(ndjson([hawk_msg]))
    check("poll: [hawk]-titled messages ignored (self notifications)",
          d == 0 and applied == [] and lg == [])

    # 6i disabled monitored session is not a candidate
    d, lg, _, _ = run_once(ndjson([msg("m1", "go")]),
                           cfg={"monitored_sessions":
                                {"sX": {"enabled": False,
                                        "project_dir": "/p"}}})
    check("poll: disabled session not a routing candidate",
          d == 0 and applied == []
          and lg and lg[-1]["kind"] == "reply_unrouted")

    # 6j a stale awaiting entry for a session disabled since its escalation
    # must not hijack the reply: it is pruned, the reply goes 1:1 into the
    # enabled session behind the message the user actually answered.
    d, lg, _, _ = run_once(
        ndjson([msg("m1", "hello")]),
        cfg={"monitored_sessions":
             {"sOld": {"enabled": False, "project_dir": "/old"},
              "sNew": {"enabled": True, "project_dir": "/new"}}},
        seed_map={"_ntfy_awaiting": ["sOld", "sNew"]})
    check("poll: stale disabled awaiting dropped, reply to enabled session",
          d == 1 and applied == [("sNew", "hello")]
          and lg and lg[-1]["kind"] == "reply"
          and lg[-1]["session"] == "sNew"[-8:])

    # 6k with several valid awaiting sessions the newest escalation wins --
    # the user answers the latest message pushed, not the oldest one.
    d, lg, _, _ = run_once(
        ndjson([msg("m1", "hello")]),
        cfg={"monitored_sessions":
             {"s1": {"enabled": True, "project_dir": "/p1"},
              "s2": {"enabled": True, "project_dir": "/p2"}}},
        seed_map={"_ntfy_awaiting": ["s1", "s2"]})
    check("poll: newest awaiting escalation wins",
          d == 1 and applied == [("s2", "hello")]
          and lg and lg[-1]["session"] == "s2"[-8:])

    # 6l apply_reply returning False is a failed delivery, not a success:
    # the session is re-queued (newest) and the reply is not logged as sent.
    def apply_bool(sid, text):
        applied.append((sid, text))
        return False
    final_map = {}
    d, lg, _, _ = run_once(
        ndjson([msg("m1", "hello")]),
        seed_map={"_ntfy_awaiting": ["sidAAA"]},
        apply_fn=apply_bool, out_map=final_map)
    check("poll: apply_reply False -> re-queued, not logged delivered",
          d == 0 and applied == [("sidAAA", "hello")]
          and final_map.get("_ntfy_awaiting") == ["sidAAA"]
          and not [e for e in lg if e["kind"] == "reply"])


def test_escalation_awaiting_prune():
    """A fresh escalation must drop stale awaiting entries for sessions the
    user has since disabled, then register its own session."""
    tmp = Path(tempfile.mkdtemp(prefix="ntfy-esc-"))
    orig_home = hawk.home_dir
    hawk.home_dir = lambda: tmp
    orig_loadcfg = notify.load_config
    cfg = {"notify": {"ntfy": {"topic": "t", "server": "https://ntfy.sh",
                               "priority": 3, "callback_base": ""}},
           "monitored_sessions": {"sOld": {"enabled": False},
                                  "sNew": {"enabled": True}}}
    notify.load_config = lambda: cfg
    orig_post = notify._post_ntfy
    orig_urlopen = urllib.request.urlopen
    # delegate to the real _post_ntfy with a stubbed HTTP layer, so the
    # push + self-id bookkeeping really run
    urllib.request.urlopen = lambda req, timeout=None: FakeResp(
        json.dumps({"id": "t1"}))
    try:
        notify._save_map({"_ntfy_awaiting": ["sOld", "sOther"]})
        session = {"id": "sNew", "title": "new"}
        ok = notify.notify_escalation(cfg, session, "stall", ["R1"], {}, "")
        m = notify._load_map()
        # sOld (explicitly disabled) pruned; sOther has no config entry (a
        # legacy single-session target / subagent) and is conservatively
        # kept; the fresh escalation's sNew is appended last (newest).
        check("escalation: registers session, prunes stale disabled entry",
              ok is True and m.get("_ntfy_awaiting") == ["sOther", "sNew"])
    finally:
        hawk.home_dir = orig_home
        notify.load_config = orig_loadcfg
        notify._post_ntfy = orig_post
        urllib.request.urlopen = orig_urlopen
        shutil.rmtree(tmp, ignore_errors=True)


def test_remove_log_entries():
    tmp = Path(tempfile.mkdtemp(prefix="ntfy-log-"))
    orig_home = hawk.home_dir
    hawk.home_dir = lambda: tmp
    try:
        notify.append_notify_log({"dir": "sent", "id": "a1", "ts": 1})
        notify.append_notify_log({"dir": "sent", "id": "a2", "ts": 2})
        notify.append_notify_log({"dir": "received", "ts": 3})
        n = notify.remove_log_entries(
            lambda e: e.get("dir") == "sent" and e.get("id") == "a1")
        check("remove_log_entries: removes matching, keeps rest",
              n == 1 and [e.get("id") for e in notify.load_notify_log()]
              == ["a2", None])
        before = (tmp / "notify_log.json").read_bytes()
        n0 = notify.remove_log_entries(lambda e: False)
        check("remove_log_entries: no match -> no rewrite, byte-identical",
              n0 == 0 and (tmp / "notify_log.json").read_bytes() == before)
        bad = b"{not valid json"
        (tmp / "notify_log.json").write_bytes(bad)
        nc = notify.remove_log_entries(lambda e: True)
        check("remove_log_entries: corrupt file not wiped as []",
              nc == 0 and (tmp / "notify_log.json").read_bytes() == bad)
    finally:
        hawk.home_dir = orig_home
        shutil.rmtree(tmp, ignore_errors=True)


def test_delete_ops():
    cfg = {"notify": {"ntfy": {"topic": "t", "server": "https://ntfy.sh"}}}
    calls = []

    def rec(req, timeout=None):
        method = getattr(req, "method", None) or "GET"
        calls.append((method, req.full_url))
        if method == "DELETE":
            return FakeResp(json.dumps({"id": "del-x"}), 200)
        return FakeStreamResp([
            '{"event":"open","id":"o"}',
            '{"event":"message","id":"ida","message":"A"}',
            '{"event":"message","id":"idb","message":"B"}',
            '{"event":"message_delete","id":"ida"}',
            '{"event":"close","id":"c"}'])

    def boom_404(req, timeout=None):
        raise urllib.error.HTTPError("u", 404, "gone", {}, None)

    def boom_500(req, timeout=None):
        raise urllib.error.HTTPError("u", 500, "boom", {}, None)

    def boom_net(req, timeout=None):
        raise urllib.error.URLError("boom")

    orig = urllib.request.urlopen
    urllib.request.urlopen = rec
    try:
        check("delete: single message ok",
              notify.delete_message(cfg, "abc123") is True
              and calls == [("DELETE", "https://ntfy.sh/t/abc123")])
        calls.clear()
        r = notify.delete_all(cfg)
        check("delete_all: enumerates then deletes each message id",
              r == {"ok": True, "deleted": 2, "total": 2,
                    "ok_ids": ["ida", "idb"], "failed_ids": []}
              and calls == [("GET", "https://ntfy.sh/t/json?poll=1&since=all"),
                            ("DELETE", "https://ntfy.sh/t/ida"),
                            ("DELETE", "https://ntfy.sh/t/idb")])
        urllib.request.urlopen = boom_404
        check("delete: HTTP 404 -> gone is success",
              notify.delete_message(cfg, "abc123") is True)
        urllib.request.urlopen = boom_500
        check("delete: HTTP error -> False",
              notify.delete_message(cfg, "abc123") is False)
    finally:
        urllib.request.urlopen = orig

    # partial failure mid-clear: only confirmed deletes are counted
    def rec_partial(req, timeout=None):
        method = getattr(req, "method", None) or "GET"
        if method == "DELETE":
            if req.full_url.endswith("/idb"):
                raise urllib.error.HTTPError("u", 500, "boom", {}, None)
            return FakeResp(json.dumps({"id": "del-x"}), 200)
        return FakeStreamResp([
            '{"event":"message","id":"ida","message":"A"}',
            '{"event":"message","id":"idb","message":"B"}'])

    urllib.request.urlopen = rec_partial
    try:
        rp = notify.delete_all(cfg)
        check("delete_all: partial failure -> confirmed ids only",
              rp == {"ok": True, "deleted": 1, "total": 2, "ok_ids": ["ida"],
                    "failed_ids": ["idb"]})
    finally:
        urllib.request.urlopen = orig

    # 429 rate limit: waits and retries instead of failing the delete
    tries = []

    def rate_limited_once(req, timeout=None):
        tries.append(1)
        if len(tries) == 1:
            raise urllib.error.HTTPError("u", 429, "slow down", {}, None)
        return FakeResp(json.dumps({"id": "del-x"}), 200)

    orig_waits = notify.DELETE_429_WAITS_S
    notify.DELETE_429_WAITS_S = [0, 0]
    urllib.request.urlopen = rate_limited_once
    try:
        check("delete: 429 then ok -> retried success",
              notify.delete_message(cfg, "abc123") is True and len(tries) == 2)

        def always_429(req, timeout=None):
            raise urllib.error.HTTPError("u", 429, "slow down", {}, None)
        urllib.request.urlopen = always_429
        check("delete: persistent 429 -> False",
              notify.delete_message(cfg, "abc123") is False)
    finally:
        urllib.request.urlopen = orig
        notify.DELETE_429_WAITS_S = orig_waits

    try:
        notify.delete_message({"notify": {"ntfy": {"topic": ""}}}, "x")
        check("delete: no topic -> ValueError", False)
    except ValueError:
        check("delete: no topic -> ValueError", True)
    try:
        notify.delete_all({"notify": {"ntfy": {"topic": ""}}})
        check("delete_all: no topic -> ValueError", False)
    except ValueError:
        check("delete_all: no topic -> ValueError", True)

    orig2 = urllib.request.urlopen
    urllib.request.urlopen = boom_net
    try:
        try:
            notify.delete_all(cfg)
            check("delete_all: enumeration error raises", False)
        except urllib.error.URLError:
            check("delete_all: enumeration error raises", True)
    finally:
        urllib.request.urlopen = orig2


# ------------------------------------------- T1b _latin1_safe header helper
def test_latin1_safe():
    check("latin1_safe: ellipsis maps to three dots",
          notify._latin1_safe("start…end") == "start...end")
    check("latin1_safe: ASCII unchanged",
          notify._latin1_safe("hello world") == "hello world")
    check("latin1_safe: latin-1 char (é) unchanged",
          notify._latin1_safe("café") == "café")
    check("latin1_safe: non-latin-1 char replaced with ?",
          notify._latin1_safe("coffee ☕") == "coffee ?")
    check("latin1_safe: empty/None -> empty",
          notify._latin1_safe("") == "" and notify._latin1_safe(None) == "")


def main():
    test_kinds_enabled()
    test_latin1_safe()
    test_record_event_gates()
    test_llama_kind()
    test_cooldown()
    test_watermarks()
    test_resolve_pending()
    test_poll_branches()
    test_escalation_awaiting_prune()
    test_remove_log_entries()
    test_delete_ops()
    print("\n%d passed, %d failed" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
