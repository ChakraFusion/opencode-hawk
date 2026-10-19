"""Self-test for the unified ntfy event pipeline.

Covers: kinds_enabled fallback, record_event gate ordering (topic -> eid
dedupe -> kinds gate -> cooldown -> burst coalescing), watermarks,
resolve_pending_route, every
reply-routing branch in poll_ntfy_replies (single awaiting, digit-resolves-
prompt, supersede, single-candidate echo, unrouted, multi-candidate prompt,
multi-awaiting prompt), screenshot discovery/upload/attachment, the ntfy
reachability record behind the web UI outage banner, reply-poll pacing
(throttle persisted across failures, exponential backoff, Retry-After), and
burst coalescing (a chatty kind inside an open window ships as one summary
push while a subagent's started/finished pair still rings twice).

Hermetic: monkey-patches the home dir, config loader, ntfy HTTP layer,
urlopen, and the reply queue so the real home state, config.json, ntfy
service, and live sessions are never touched.

Run:  python tests/selftest_notify.py     (exit 0 = all pass)
"""
import json
import os
import shutil
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from opencode_hawk import notify  # noqa: E402
from opencode_hawk import coordinator as hawk  # noqa: E402

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
    # A case that pushed a subagent opens a burst window; if it survived into
    # the next case the next case's first event would be swallowed by it.
    notify._cohort_reset()
    orig_post = notify._post_ntfy
    pushes = []

    def rec_post(c, title, body, tags="", actions=None, kind="alert",
                 eid="", meta=None, attach=""):
        pushes.append({"title": title, "body": body, "kind": kind,
                       "eid": eid, "tags": tags, "attach": attach})
        # Delegate to the real _post_ntfy (urlopen is stubbed) so the
        # log-append / self-id bookkeeping inside it actually runs.
        return orig_post(c, title, body, tags=tags, actions=actions,
                         kind=kind, eid=eid, meta=meta, attach=attach)

    notify._post_ntfy = rec_post
    orig_qr = notify.queue_reply
    queued = []
    notify.queue_reply = lambda sid, text: (queued.append((sid, text)), True)[1]
    orig_urlopen = urllib.request.urlopen
    urllib.request.urlopen = lambda req, timeout=None: FakeResp(
        json.dumps({"id": "t1"}))

    def restore():
        # Drop the burst window while home is still stubbed: a timer left
        # armed by a subagent case would otherwise flush against the real
        # config and the real ntfy topic after the test is over.
        notify._cohort_reset()
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

        # Configuring one kind's cooldown must not wipe the default for the
        # others (this silently made every unspecified kind push immediately).
        cfg_cd = {"notify": {"ntfy": {"kind_cooldown_s": {"commit": 30}}}}
        check("cooldown: unspecified kinds keep their default",
              notify.cooldown_s(cfg_cd, "commit") == 30
              and notify.cooldown_s(cfg_cd, "nudge") == 60
              and notify.cooldown_s(cfg_cd, "subagent") == 0)
        check("cooldown: explicit 0 still opts a kind out",
              notify.cooldown_s(
                  {"notify": {"ntfy": {"kind_cooldown_s": {"subagent": 0}}}},
                  "subagent") == 0)
    finally:
        r()


# -------------------------------------------- T3b burst coalescing
def test_coalesce():
    """A tree that finishes many tasks at once must not fire one POST each:
    the first event pushes, the rest of the window ships as one summary, and a
    subagent's started/finished pair still rings twice."""
    cfg = {"notify": {"ntfy": {"topic": "t", "kinds": ["subagent", "commit"]}}}
    r, pushes, _ = install(cfg)
    try:
        check("coalesce: opening event pushed individually",
              notify.record_event(cfg, "subagent", "subagent a: t1", "b",
                                  eid="sub:a:finished") is True
              and len(pushes) == 1
              and pushes[0]["eid"] == "sub:a:finished")

        got = [notify.record_event(cfg, "subagent", "subagent %s: t" % c, "b",
                                   eid="sub:%s:finished" % c)
               for c in "bcdefg"]
        check("coalesce: rest of burst collected, no extra POST",
              got == [True] * 6 and len(pushes) == 1)

        check("coalesce: collected eid remembered (duplicate dropped)",
              notify.record_event(cfg, "subagent", "subagent b: t", "b",
                                  eid="sub:b:finished") is False
              and len(pushes) == 1)

        check("coalesce: flush sends one summary naming the count",
              notify._flush_cohort() is True and len(pushes) == 2
              and pushes[-1]["kind"] == "subagent"
              and "6 subagent event" in pushes[-1]["title"]
              and "- subagent b: t" in pushes[-1]["body"]
              and "robot" in (pushes[-1]["tags"] or ""))

        check("coalesce: second flush has nothing left",
              notify._flush_cohort() is False and len(pushes) == 2)

        # started + finished of one task both ring (subagent_state_eids); only
        # ONE such exemption exists per window, so an entity cannot stream.
        notify.record_event(cfg, "subagent", "subagent h: t8", "started",
                            eid="sub:h:started", meta={"state": "started"})
        notify.record_event(cfg, "subagent", "subagent h: t8", "finished",
                            eid="sub:h:finished", meta={"state": "finished"})
        check("coalesce: started/finished pair both pushed",
              len(pushes) == 4
              and [p["eid"] for p in pushes[2:4]] == ["sub:h:started",
                                                     "sub:h:finished"])
        check("coalesce: third event for that entity is collected",
              notify.record_event(cfg, "subagent", "subagent h: t8", "again",
                                  eid="sub:h:requeued") is True
              and len(pushes) == 4)

        check("coalesce: other kinds push normally inside a window",
              notify.record_event(cfg, "commit", "c", "b", eid="c1") is True
              and len(pushes) == 5)

        # The cap closes the window rather than collecting without bound: the
        # overflow event is pushed on its own so it is never dropped.
        notify._cohort_reset()
        orig_cap = notify.COALESCE_MAX_ITEMS
        notify.COALESCE_MAX_ITEMS = 3
        try:
            notify.record_event(cfg, "subagent", "s0", "b", eid="sub:z0:finished")
            n0 = len(pushes)
            for i in range(1, 6):
                notify.record_event(cfg, "subagent", "s%d" % i, "b",
                                    eid="sub:z%d:finished" % i)
            check("coalesce: cap flushes instead of collecting forever",
                  len(pushes) == n0 + 2
                  and "3 subagent event" in pushes[-2]["title"]
                  and pushes[-1]["eid"] == "sub:z4:finished")
        finally:
            notify.COALESCE_MAX_ITEMS = orig_cap

        # A window that outlived its process is finished by the next event
        # instead of losing the burst that was collected before the restart.
        notify._cohort_reset()
        stale = notify._cohort_empty()
        stale["kind"] = "subagent"
        stale["since"] = notify._time.time() - notify.COALESCE_WINDOW_S - 1
        stale["items"] = [{"title": "[hawk] subagent old: t", "eid": "sub:o:finished"}]
        hawk.save_json(notify._cohort_path(), stale)
        n1 = len(pushes)
        notify.record_event(cfg, "subagent", "subagent new: t", "b",
                            eid="sub:n:finished")
        check("coalesce: expired window flushed after a restart",
              len(pushes) == n1 + 2
              and "1 subagent event" in pushes[-2]["title"]
              and pushes[-1]["eid"] == "sub:n:finished")

        # A restart INSIDE the window inherits it but not its timer, so the
        # next event must re-arm one - otherwise the summary waits forever for
        # an event that may never come.
        notify._cohort_reset()
        live = notify._cohort_empty()
        live["kind"] = "subagent"
        live["since"] = notify._time.time() - 5      # 55s of window left
        live["items"] = [{"title": "[hawk] subagent pending: t", "eid": "sub:p:finished"}]
        hawk.save_json(notify._cohort_path(), live)
        n2 = len(pushes)
        check("coalesce: in-window restart re-arms a timer",
              notify.record_event(cfg, "subagent", "subagent q: t", "b",
                                  eid="sub:q:finished") is True
              and len(pushes) == n2
              and notify._cohort_timer is not None)
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
                  and "1. p1 [s1]" in x["body"]
                  and "2. p2 [s2]" in x["body"]
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

    # 6k with several valid awaiting sessions the reply is ambiguous: ntfy
    # must ask which session instead of silently picking one, and the awaiting
    # FIFO is cleared because the pending prompt now owns the choice.
    amap = {}
    d, lg, pushes, pend = run_once(
        ndjson([msg("m1", "hello")]),
        cfg={"monitored_sessions":
             {"s1": {"enabled": True, "project_dir": "/p1"},
              "s2": {"enabled": True, "project_dir": "/p2"}}},
        seed_map={"_ntfy_awaiting": ["s1", "s2"]}, out_map=amap)
    check("poll: multiple awaiting -> numbered prompt, awaiting cleared",
          d == 0 and applied == []
          and [c.get("sid") for c in pend.get("candidates") or []]
              == ["s1", "s2"]
          and amap.get("_ntfy_awaiting") == []
          and any(x["kind"] == "routing" and "1. p1 [s1]" in x["body"]
                  and "2. p2 [s2]" in x["body"] for x in pushes))

    # 6k-bis the prompt 6k saved resolves with a digit on the next reply.
    d, lg, _, pend = run_once(ndjson([msg("m2", "2")]), pending=dict(pend))
    check("poll: awaited-prompt digit routes original text",
          d == 1 and applied == [("s2", "hello")] and pend == {}
          and lg and lg[-1].get("note") == "routed via prompt #2")

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


def test_screenshots():
    # _screenshot_cfg: defaults on, 30 min, known dirs; explicit off respected.
    sc = notify._screenshot_cfg({"notify": {"ntfy": {"topic": "t"}}})
    check("shots: defaults (on, 30min, known dirs, 2MB)",
          sc["enabled"] is True and sc["max_age_s"] == 1800
          and ".playwright-mcp" in sc["dirs"] and sc["max_bytes"] == 2_000_000)
    check("shots: attach_screenshots=false respected",
          notify._screenshot_cfg(
              {"notify": {"ntfy": {"attach_screenshots": False}}})["enabled"]
          is False)

    root = Path(tempfile.mkdtemp(prefix="shots-"))
    try:
        (root / "new.png").write_bytes(b"x" * 32)
        stale = root / "stale.png"
        stale.write_bytes(b"x" * 32)
        os.utime(stale, (0, 0))           # epoch -> outside the age window
        (root / "note.txt").write_bytes(b"x" * 32)
        big = root / "big.png"
        big.write_bytes(b"x" * 5000)
        sub = root / ".playwright-mcp"
        sub.mkdir()
        (sub / "shot.webp").write_bytes(b"x" * 32)
        (sub / "p5-1.jpg").write_bytes(b"x" * 32)

        shots = notify.find_recent_screenshots(
            str(root), max_age_s=1800, dirs=[".playwright-mcp"])
        names = {p.name for p in shots}
        check("shots: recent images found (root + known dir), old/junk skipped",
              names == {"new.png", "big.png", "shot.webp", "p5-1.jpg"}
              and "stale.png" not in names and "note.txt" not in names
              and all(p.suffix.lower() in notify.SCREENSHOT_EXTS
                      for p in shots))

        shots2 = notify.find_recent_screenshots(
            str(root), max_age_s=1800, dirs=[".playwright-mcp"], max_bytes=1000)
        check("shots: oversize images skipped",
              all(p.name != "big.png" for p in shots2))

        check("shots: missing/empty project_dir -> []",
              notify.find_recent_screenshots("") == []
              and notify.find_recent_screenshots(str(root / "nope")) == [])
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_attach_upload():
    cfg = {"notify": {"ntfy": {"topic": "t", "server": "https://ntfy.sh"}}}
    restore, pushes, _ = install(cfg)
    root = Path(tempfile.mkdtemp(prefix="att-"))
    try:
        img = root / "shot.png"
        img.write_bytes(b"PNGDATA")
        cap = {}

        def fake(req, timeout=None):
            cap["method"] = req.get_method()
            cap["url"] = req.full_url
            cap["filename"] = req.get_header("Filename")
            cap["data"] = req.data
            if req.get_method() == "PUT":
                return FakeResp(json.dumps(
                    {"attachment": {"url": "https://ntfy.sh/att/abc"}}))
            return FakeResp(json.dumps({"id": "z1"}))

        urllib.request.urlopen = fake
        url = notify.upload_attachment(cfg, img)
        check("attach: PUT upload returns the served attachment url",
              url == "https://ntfy.sh/att/abc"
              and cap["method"] == "PUT" and cap["filename"] == "shot.png"
              and cap["url"] == "https://ntfy.sh/t")

        cap.clear()
        ok = notify._post_ntfy(cfg, "[hawk] t", "body", kind="routing",
                               attach=url)
        payload = json.loads(cap["data"].decode("utf-8"))
        check("attach: _post_ntfy carries the url in the JSON publish",
              ok and payload.get("attach") == "https://ntfy.sh/att/abc")

        urllib.request.urlopen = lambda req, timeout=None: FakeResp(b"nope")
        check("attach: unparseable upload -> '' (text-only push)",
              notify.upload_attachment(cfg, img) == "")

        # Escalation end-to-end: find -> upload -> record_event(attach).
        orig_find = notify.find_recent_screenshots
        orig_up = notify.upload_attachment
        notify.find_recent_screenshots = lambda *a, **k: [Path("shot.png")]
        notify.upload_attachment = lambda c, p: "https://ntfy.sh/att/e.png"
        try:
            sent = notify.notify_escalation(
                cfg, {"id": "sidX", "title": "T"}, "why", [],
                {"last_assistant": ""}, "/rep", project_dir="/proj")
        finally:
            notify.find_recent_screenshots = orig_find
            notify.upload_attachment = orig_up
        check("attach: escalation push carries the screenshot url",
              sent and any(x.get("attach") == "https://ntfy.sh/att/e.png"
                           for x in pushes))
    finally:
        shutil.rmtree(root, ignore_errors=True)
        restore()


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


def test_fit_ntfy():
    short = "All work is complete.\n" * 20
    check("fit_ntfy: a message that fits is sent whole", notify.fit_ntfy(short) == short)
    long = "\n".join("line %d: verified feature with a long description" % i for i in range(400))
    out = notify.fit_ntfy(long)
    check("fit_ntfy: a long message is cut to ntfy's limit at a line end",
          len(out.encode("utf-8")) <= notify.NTFY_MAX_BYTES and out.endswith("(full text in the Hawk dashboard)")
          and long.startswith(out.split("\n\u2026")[0]))


def test_log_write_failure_is_contained():
    # A failed log write (e.g. the dashboard writing the same file) must be logged, not raised:
    # the local list used to shadow log(), so the handler itself crashed and a pushed alert looked unsent.
    orig = notify._write_log
    notify._write_log = lambda entries: (_ for _ in ()).throw(PermissionError("busy"))
    try:
        ok1 = ok2 = True
        try:
            notify.append_notify_log({"dir": "sent", "id": "x", "ts": 1})
        except Exception:
            ok1 = False
        try:
            notify.remove_log_entries(lambda e: True)
        except Exception:
            ok2 = False
    finally:
        notify._write_log = orig
    check("log write failure: append_notify_log does not raise", ok1)
    check("log write failure: remove_log_entries does not raise", ok2)


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
        urllib.request.urlopen = boom_404
        check("delete: HTTP 404 -> gone is success",
              notify.delete_message(cfg, "abc123") is True)
        urllib.request.urlopen = boom_500
        check("delete: HTTP error -> False",
              notify.delete_message(cfg, "abc123") is False)
    finally:
        urllib.request.urlopen = orig

    # clear-all returns at once; the server deletes run in the background, a failing one does not stop
    # the others, and only live messages inside ntfy's 12 h cache are deleted
    import time as _tm
    now_s = int(_tm.time())
    tmp_home = Path(tempfile.mkdtemp(prefix="ntfy-clear-"))
    orig_home = hawk.home_dir
    hawk.home_dir = lambda: tmp_home
    deletes = []

    def rec_bg(req, timeout=None):
        method = getattr(req, "method", None) or "GET"
        if method == "DELETE":
            deletes.append(req.full_url)
            if req.full_url.endswith("/idb"):
                raise urllib.error.HTTPError("u", 500, "boom", {}, None)
            return FakeResp(json.dumps({"id": "del-x"}), 200)
        return FakeStreamResp([
            '{"event":"message","id":"ida","message":"A","time":%d}' % (now_s - 60),
            '{"event":"message","id":"idb","message":"B","time":%d}' % (now_s - 30),
            '{"event":"message","id":"idc","message":"C","time":%d}' % (now_s - 20),
            '{"event":"message","id":"old","message":"O","time":%d}' % (now_s - 13 * 3600)])

    urllib.request.urlopen = rec_bg
    try:
        t0 = _tm.time()
        rp = notify.delete_all(cfg)
        quick = _tm.time() - t0 < 2.0
        th = notify._bg["thread"]
        if th:
            th.join(timeout=10)
        check("delete_all: returns at once, hides all live, queues the young ones",
              quick and rp == {"ok": True, "hidden": 4, "queued": 3}
              and notify.hidden_ids() >= {"ida", "idb", "idc", "old"})
        check("delete_all: background deletes continue past a failure, skip expired",
              sorted(deletes) == ["https://ntfy.sh/t/ida", "https://ntfy.sh/t/idb", "https://ntfy.sh/t/idc"])
    finally:
        urllib.request.urlopen = orig
        hawk.home_dir = orig_home
        shutil.rmtree(tmp_home, ignore_errors=True)

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


def test_send_429_retry():
    """The publish path backs off and retries on 429 like the delete path
    (ntfy's per-client bucket is shared with hawk's own channel polling)."""
    tmp = Path(tempfile.mkdtemp(prefix="ntfy-send429-"))
    orig_home = hawk.home_dir
    hawk.home_dir = lambda: tmp
    orig_loadcfg = notify.load_config
    cfg = {"notify": {"ntfy": {"topic": "t", "server": "https://ntfy.sh",
                               "priority": 3}}, "monitored_sessions": {}}
    notify.load_config = lambda: cfg
    orig_waits = notify.NTFY_429_WAITS_S
    notify.NTFY_429_WAITS_S = [0, 0]
    orig_urlopen = urllib.request.urlopen
    tries = []

    def flaky(req, timeout=None):
        tries.append(1)
        if len(tries) == 1:
            raise urllib.error.HTTPError("u", 429, "slow", {}, None)
        return FakeResp(json.dumps({"id": "t1"}), 200)

    exhausted = []

    def always_429(req, timeout=None):
        exhausted.append(1)
        raise urllib.error.HTTPError("u", 429, "slow", {}, None)

    try:
        urllib.request.urlopen = flaky
        ok1 = notify._post_ntfy(cfg, "title", "body")
        check("send: 429 then 200 retries and succeeds",
              ok1 is True and len(tries) == 2)
        urllib.request.urlopen = always_429
        ok2 = notify._post_ntfy(cfg, "title", "body")
        check("send: exhausted 429 -> False after full backoff",
              ok2 is False and len(exhausted) == len(notify.NTFY_429_WAITS_S) + 1)
    finally:
        urllib.request.urlopen = orig_urlopen
        notify.NTFY_429_WAITS_S = orig_waits
        notify.load_config = orig_loadcfg
        hawk.home_dir = orig_home
        shutil.rmtree(tmp, ignore_errors=True)


def test_ntfy_health():
    """ntfy_health() derives the web UI outage banner from the last real
    exchange; it never touches the network and never raises."""
    tmp = Path(tempfile.mkdtemp(prefix="ntfy-health-"))
    orig_home = hawk.home_dir
    hawk.home_dir = lambda: tmp
    orig_urlopen = urllib.request.urlopen
    try:
        cfg = {"notify": {"ntfy": {"topic": "t", "server": "https://ntfy.sh"}}}

        # Any network attempt here would raise (no real I/O is expected).
        def blowup(req, timeout=None):
            raise AssertionError("ntfy_health must not touch the network")
        urllib.request.urlopen = blowup

        # No topic -> not configured, never warns.
        h = notify.ntfy_health({"notify": {"ntfy": {"topic": ""}}})
        check("health: unconfigured topic",
              h["configured"] is False and h["reachable"] is True)

        # No record yet (fresh install) -> reachable, no false alarm.
        check("health: no record -> reachable, source none",
              notify.read_ntfy_health() == {}
              and notify.ntfy_health(cfg)["source"] == "none"
              and notify.ntfy_health(cfg)["reachable"] is True)

        # A failed exchange -> unreachable, error + since surfaced.
        notify.record_ntfy_exchange(False, "<urlopen error timed out>")
        h = notify.ntfy_health(cfg)
        check("health: failure -> unreachable",
              h["reachable"] is False and h["source"] == "exchange"
              and "timed out" in h["error"] and h["since_ms"] > 0
              and h["stale"] is False)

        # A later success clears the warning and keeps last_ok_ms.
        notify.record_ntfy_exchange(True)
        h = notify.ntfy_health(cfg)
        check("health: success -> reachable, last_ok_ms set",
              h["reachable"] is True and h["source"] == "exchange"
              and h["last_ok_ms"] > 0 and h["error"] == "")

        # A very old record is flagged stale (still reports the last state).
        (notify._health_path()).write_text(
            json.dumps({"ok": False, "ts": 1, "error": "old",
                        "last_ok_ms": 5}), encoding="utf-8")
        h = notify.ntfy_health(cfg)
        check("health: old record flagged stale",
              h["stale"] is True and h["last_ok_ms"] == 5)
    finally:
        urllib.request.urlopen = orig_urlopen
        hawk.home_dir = orig_home
        shutil.rmtree(tmp, ignore_errors=True)


def test_poll_backoff():
    """poll_ntfy_replies must never hammer a failing ntfy server.

    The throttle has to be persisted *before* the request: saving only after a
    successful parse meant every failure threw the 10 s throttle away, so an
    unreachable ntfy was re-polled on every monitor loop tick (~1/s). That
    sustained burst is what tripped ntfy's rate limiter and got this machine's
    IP banned (and it recurred on each later hiccup). Failures now widen the
    gap -- honouring an explicit Retry-After -- and a success resets it."""
    tmp = Path(tempfile.mkdtemp(prefix="ntfy-backoff-"))
    orig_home = hawk.home_dir
    hawk.home_dir = lambda: tmp
    orig_loadcfg = notify.load_config
    orig_urlopen = urllib.request.urlopen
    notify.load_config = lambda: {"notify": {"ntfy": {"topic": "t",
                                                     "server": "https://ntfy.sh"}}}
    cfg = notify.load_config()
    calls = {"n": 0}

    def boom(req, timeout=None):
        calls["n"] += 1
        raise urllib.error.URLError("<urlopen error timed out>")

    def raise429(req, timeout=None):
        calls["n"] += 1
        raise urllib.error.HTTPError("https://ntfy.sh/t", 429,
                                     "Too Many Requests",
                                     {"Retry-After": "300"}, None)

    def empty(req, timeout=None):
        calls["n"] += 1
        return FakeResp("")

    def force(fn):
        """Make the next poll actually fire: rewind the persisted throttle."""
        urllib.request.urlopen = fn
        m = notify._load_map()
        m["_ntfy_polled_at"] = 0
        notify._save_map(m)
        notify.poll_ntfy_replies(lambda s, t: None)
        return notify._load_map()

    def ok_poll():
        m = force(empty)
        # A success must clear the breaker for the very next attempt.
        return m

    try:
        # ---- pacing helpers are pure and capped -------------------------
        check("poll: Retry-After read only from 429/503",
              notify._retry_after_s(urllib.error.HTTPError(
                  "u", 429, "m", {"Retry-After": "45"}, None)) == 45
              and notify._retry_after_s(urllib.error.HTTPError(
                  "u", 503, "m", {"Retry-After": "7"}, None)) == 7
              and notify._retry_after_s(urllib.error.HTTPError(
                  "u", 500, "m", {"Retry-After": "7"}, None)) is None
              and notify._retry_after_s(urllib.error.URLError(
                  "timed out")) is None
              and notify._retry_after_s(urllib.error.HTTPError(
                  "u", 429, "m", {"Retry-After": "not-a-number"}, None)) is None)

        check("poll: gap schedule, floor and Retry-After cap",
              notify._poll_gap_ms(1) == notify.NTFY_POLL_MIN_GAP_S * 1000
              and notify._poll_gap_ms(4) == notify.NTFY_POLL_BACKOFF_S[-1] * 1000
              and notify._poll_gap_ms(99) == notify.NTFY_POLL_BACKOFF_S[-1] * 1000
              and notify._poll_gap_ms(1, retry_after=45) == 45_000
              and notify._poll_gap_ms(1, retry_after=1)
              == notify.NTFY_POLL_MIN_GAP_S * 1000
              and notify._poll_gap_ms(1, retry_after=999_999)
              == notify.NTFY_POLL_RETRY_AFTER_CAP_S * 1000)

        # ---- 1. a failed attempt still throttles the next one ----------
        urllib.request.urlopen = boom
        notify._save_map({})
        notify.poll_ntfy_replies(lambda s, t: None)
        after_first = calls["n"]
        notify.poll_ntfy_replies(lambda s, t: None)
        check("poll: failure still advances the throttle",
              after_first == 1 and calls["n"] == 1,
              "after_first=%s calls=%s" % (after_first, calls["n"]))

        # ---- 2. consecutive failures widen the gap ---------------------
        m = notify._load_map()
        check("poll: first failure recorded in map",
              m.get("_ntfy_fails") == 1
              and m.get("_ntfy_gap_ms") == notify.NTFY_POLL_MIN_GAP_S * 1000,
              "fails=%s gap=%s" % (m.get("_ntfy_fails"), m.get("_ntfy_gap_ms")))

        for _ in range(3):
            force(boom)
        m = notify._load_map()
        check("poll: gap widens with consecutive failures",
              m.get("_ntfy_fails") == 4
              and m.get("_ntfy_gap_ms")
              == notify.NTFY_POLL_BACKOFF_S[-1] * 1000,
              "fails=%s gap=%s" % (m.get("_ntfy_fails"), m.get("_ntfy_gap_ms")))

        # One network attempt per poll -- no hidden in-call retry loop.
        check("poll: exactly one attempt per poll", calls["n"] == 4,
              "calls=%s" % calls["n"])

        # ---- 3. ntfy's own Retry-After is honoured ---------------------
        m = force(raise429)
        check("poll: 429 Retry-After widens the gap",
              m.get("_ntfy_fails") == 5 and m.get("_ntfy_gap_ms") == 300_000,
              "fails=%s gap=%s" % (m.get("_ntfy_fails"), m.get("_ntfy_gap_ms")))
        check("poll: health counts consecutive failures",
              notify.ntfy_health(cfg)["fails"] == 5
              and notify.ntfy_health(cfg)["reachable"] is False)

        # ---- 4. a success resets pacing to normal ----------------------
        m = ok_poll()
        check("poll: success resets breaker and gap",
              m.get("_ntfy_fails") == 0
              and m.get("_ntfy_gap_ms") == notify.NTFY_POLL_MIN_GAP_S * 1000,
              "fails=%s gap=%s" % (m.get("_ntfy_fails"), m.get("_ntfy_gap_ms")))
        h = notify.ntfy_health(cfg)
        check("poll: success clears health failure count",
              h["fails"] == 0 and h["reachable"] is True,
              "h=%s" % h)
    finally:
        urllib.request.urlopen = orig_urlopen
        notify.load_config = orig_loadcfg
        hawk.home_dir = orig_home
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    test_kinds_enabled()
    test_record_event_gates()
    test_llama_kind()
    test_cooldown()
    test_coalesce()
    test_watermarks()
    test_resolve_pending()
    test_poll_branches()
    test_screenshots()
    test_attach_upload()
    test_escalation_awaiting_prune()
    test_remove_log_entries()
    test_log_write_failure_is_contained()
    test_fit_ntfy()
    test_delete_ops()
    test_send_429_retry()
    test_ntfy_health()
    test_poll_backoff()
    print("\n%d passed, %d failed" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
