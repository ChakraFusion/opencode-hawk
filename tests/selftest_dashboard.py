#!/usr/bin/env python3
"""Dashboard self-test: pure providers plus a local HTTP round trip.

Run directly (`python tests/selftest_dashboard.py`) or through
`python dashboard.py --self-test`.

The checks were written inside dashboard.py and use its names unqualified,
so the module's namespace is mirrored here. Module state the checks rebind
(`dash._tele_series`, `dash._LLAMA_TASKS`, patched functions via
`vars(dash)`) is addressed on the dashboard module itself.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from opencode_hawk import dashboard as dash  # noqa: E402

globals().update({k: v for k, v in vars(dash).items()
                  if not (k.startswith("__") and k.endswith("__"))})


def self_test():
    """Unit-dashboard checks: pure providers, no network."""
    ok = True

    def check(name, cond):
        nonlocal ok
        ok = ok and bool(cond)
        print("%-28s %s  %s" % (name, "OK" if cond else "FAIL", repr(cond)))

    # control.json seq roundtrip
    tmp = Path(tempfile.mkdtemp())
    old_home = coordinator.home_dir
    coordinator.home_dir = lambda: tmp
    try:
        d1 = write_control("pause")
        assert d1["seq"] == 1 and d1["intent"] == "pause"
        d2 = write_control("resume")
        check("control_seq_roundtrip", d2["seq"] == 2 and d2["intent"] == "resume")
        check("control_file_written", (tmp / "control.json").exists())
    finally:
        coordinator.home_dir = old_home
        shutil.rmtree(tmp, ignore_errors=True)

    # git_log_events on a temp repo
    repo = Path(tempfile.mkdtemp()); (repo / "f").mkdir()
    (repo / "f" / "a.txt").write_text("a", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "hello hawk"], check=True)
    evs = git_log_events(str(repo), n=5)
    check("git_log_events", len(evs) >= 1 and evs[0]["kind"] == "commit"
          and "hello hawk" in evs[0]["title"] and evs[0]["time"] > 0)
    shutil.rmtree(repo, ignore_errors=True)

    # milestone_events on a temp sqlite replica of message/part
    db = sqlite3.connect(":memory:")
    db.executescript(
        "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INT, data TEXT, time_updated INT);"
        "CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INT, time_updated INT, data TEXT);"
    )
    db.execute("INSERT INTO message VALUES ('m1','ses','1000','{\"role\":\"assistant\"}','1000')")
    db.execute("INSERT INTO part VALUES ('p1','m1','ses','2000','2000','{\"type\":\"text\",\"text\":\"done. STOP: VERIFIED\\nRESULT: PASS tests=444\"}')")
    db.execute("INSERT INTO part VALUES ('p2','m1','ses','3000','3000','{\"type\":\"reasoning\",\"text\":\"planning\"}')")
    db.execute("INSERT INTO message VALUES ('m2','ses','4000','{\"role\":\"assistant\"}','4000')")
    db.execute("INSERT INTO part VALUES ('p3','m2','ses','5000','5000','{\"type\":\"text\",\"text\":\"## Objective\\n- Implement chunk C8.4b popups\\nSTOP: DONE\"}')")
    db.execute("INSERT INTO message VALUES ('m3','ses','6000','{\"role\":\"assistant\"}','6000')")
    db.execute("INSERT INTO part VALUES ('p4','m3','ses','7000','7000','{\"type\":\"text\",\"text\":\"\\\"[coordinator] Milestone verified. Continue ... STOP: VERIFIED\\\"\"}')")
    ms = milestone_events(db, "ses")
    check("milestone_events", len(ms) == 2 and ms[0]["kind"] == "milestone"
          and ms[0]["ok"] and ms[0]["tests"] == 444 and ms[0]["time"] == 2000
          and ms[0]["title"] == "STOP: VERIFIED"
          and ms[1]["title"] == "Implement chunk C8.4b popups" and ms[1]["ok"]
          and ms[0]["sid"] == "ses")

    # All-sessions aggregation: empty session_id spans every session, while a
    # concrete id still scopes to that one (the frontend's "All sessions" view).
    db.execute("INSERT INTO message VALUES ('mx1','ses2','15000','{\"role\":\"assistant\"}','15000')")
    db.execute("INSERT INTO part VALUES ('px1','mx1','ses2','15100','15100','{\"type\":\"text\",\"text\":\"Cross-session milestone. STOP: VERIFIED\"}')")
    ms_all = milestone_events(db, "")
    ms_s1 = milestone_events(db, "ses")
    check("milestone_events_all", len(ms_all) == 3
          and any(e["time"] == 15100 for e in ms_all)
          and len(ms_s1) == 2)

    # last_evaluated_summary: readable subject, [coordinator] skip, empty
    db.execute("INSERT INTO message VALUES ('m4','ses','8000','{\"role\":\"assistant\"}','8000')")
    db.execute("INSERT INTO part VALUES ('p6','m4','ses','8500','8500','{\"type\":\"text\",\"text\":\"Milestone C8.4b complete \\u2014 committed as `f8b0967`.\"}')")
    db.execute("INSERT INTO part VALUES ('p7','m4','ses','8600','8600','{\"type\":\"reasoning\",\"text\":\"internal reasoning\"}')")
    db.execute("INSERT INTO message VALUES ('m5','ses','9000','{\"role\":\"assistant\"}','9000')")
    db.execute("INSERT INTO part VALUES ('p8','m5','ses','9100','9100','{\"type\":\"reasoning\",\"text\":\"The spec is now confirmed here.\"}')")
    check("last_evaluated_summary",
          last_evaluated_summary(db, "m4") == "Milestone C8.4b complete — committed as `f8b0967`."
          and last_evaluated_summary(db, "m3").startswith("m3")
          and last_evaluated_summary(db, "m5") == "The spec is now confirmed here."
          and last_evaluated_summary(db, "") == "—"
          and last_evaluated_summary(db, None) == "—")

    # continue_events classifier: legacy [coordinator] marker AND the plain
    # configured hawk messages (which no longer carry the marker) both count as
    # injections; ordinary user messages are filtered out.
    cmsg = str(coordinator.load_config().get("continue_message") or "CONT")
    db.execute("INSERT INTO message VALUES ('cu1','ses','11000','{\"role\":\"user\"}','11000')")
    db.execute("INSERT INTO part VALUES ('pp1','cu1','ses','11100','11100',?)",
               (json.dumps({"type": "text", "text": '"' + cmsg[:60] + '"'}),))
    db.execute("INSERT INTO message VALUES ('cu2','ses','12000','{\"role\":\"user\"}','12000')")
    db.execute("INSERT INTO part VALUES ('pp2','cu2','ses','12100','12100','{\"type\":\"text\",\"text\":\"[coordinator] Please continue the milestone...\"}')")
    db.execute("INSERT INTO message VALUES ('cu3','ses','13000','{\"role\":\"user\"}','13000')")
    db.execute("INSERT INTO part VALUES ('pp3','cu3','ses','13100','13100','{\"type\":\"text\",\"text\":\"Fix the login bug please\"}')")
    ce = continue_events(db, "ses")
    check("continue_events",
          len(ce) == 2 and {e["kind"] for e in ce} == {"continue"}
          and ce[0]["time"] == 11100 and ce[1]["time"] == 12100)
    # continue/subagent aggregation across sessions with an empty id
    db.execute("INSERT INTO message VALUES ('cu4','ses2','16000','{\"role\":\"user\"}','16000')")
    db.execute("INSERT INTO part VALUES ('pp4','cu4','ses2','16100','16100',?)",
               (json.dumps({"type": "text", "text": cmsg[:60]}),))
    db.execute("INSERT INTO part VALUES ('ta1','cu4','ses2','16200','16200',?)",
               (json.dumps({"type": "tool", "tool": "task", "state": {
                   "status": "completed",
                   "time": {"start": 16200, "end": 16400},
                   "input": {"subagent_type": "explore", "description": "probe"}}}),))
    ce_all = continue_events(db, "")
    ce_s1 = continue_events(db, "ses")
    sa_all = subagent_events(db, "")
    sa_s1 = subagent_events(db, "ses")
    check("continue_events_all", len(ce_all) == 3 and len(ce_s1) == 2)
    check("subagent_events_all", len(sa_all) == 1 and len(sa_s1) == 0
          and sa_all[0]["kind"] == "subagent"
          and "explore" in sa_all[0]["title"])

    # every event says which session it came from: milestone/subagent events
    # carry their source sid, and timeline_events resolves a display label
    # from session titles (short id tail when the session has no title)
    db.execute("CREATE TABLE IF NOT EXISTS session "
               "(id TEXT PRIMARY KEY, parent_id TEXT, title TEXT)")
    db.execute("INSERT OR IGNORE INTO session VALUES "
               "('ses', NULL, 'Demo plan'), ('ses2', NULL, '')")
    tls = timeline_events("", "", db)
    msess = {e["sid"] for e in tls if e["kind"] == "milestone"}
    lbl = {e["sid"]: e["sess"] for e in tls if e["kind"] == "milestone"}
    check("timeline_event_sessions", msess == {"ses", "ses2"}
          and lbl.get("ses") == "Demo plan"
          and bool(lbl.get("ses2")) and lbl.get("ses2") != lbl.get("ses")
          and any(e["kind"] == "subagent" and e["sid"] == "ses2" for e in tls))
    db.close()

    # settings: clamp, unknown-key ignore, atomic write to config.json
    tmp = Path(tempfile.mkdtemp())
    old_home = coordinator.home_dir
    coordinator.home_dir = lambda: tmp
    try:
        coordinator.save_json(tmp / "config.json",
                              {"session_id": "s", "poll_minutes": 3,
                               "idle_minutes": 15, "stalled_turn_minutes": 30})
        upd = update_settings({"poll_minutes": 12, "idle_minutes": 999,
                               "stalled_turn_minutes": -5,
                               "re_stall_minutes": 9999, "bogus": 7})
        check("settings_update_clamp", upd.get("poll_minutes") == 12
              and upd.get("idle_minutes") == 720 and upd.get("stalled_turn_minutes") == 1
              and upd.get("re_stall_minutes") == 720 and "bogus" not in upd)
        cfg = coordinator.load_config()
        check("settings_persisted",
              cfg.get("poll_minutes") == 12 and cfg.get("idle_minutes") == 720
              and cfg.get("stalled_turn_minutes") == 1
              and cfg.get("re_stall_minutes") == 720 and cfg.get("session_id") == "s")
        payload = settings_defaults()
        check("settings_defaults", payload["poll_minutes"] == 12
              and payload["idle_minutes"] == 720
              and "re_stall_minutes" in payload
              and "stall_llama_window_s" in payload)
        try:
            update_settings({"poll_minutes": "abc"})
            check("settings_bad_value", False)
        except ValueError:
            check("settings_bad_value", True)
        updk = update_settings({"llama_kill_degraded": True,
                                "llama_kill_degraded_s": 300,
                                "llama_kill_tps": 8.5})
        check("kill_settings_updated",
              updk.get("llama_kill_degraded") is True
              and updk.get("llama_kill_degraded_s") == 300
              and updk.get("llama_kill_tps") == 8.5
              and coordinator.load_config()["llama_kill_degraded"] is True)
        updk2 = update_settings({"llama_kill_degraded_s": 999999,
                                 "llama_kill_tps": 999})
        check("kill_settings_clamp",
              updk2.get("llama_kill_degraded_s") == 3600
              and updk2.get("llama_kill_tps") == 60.0)
        try:
            update_settings({"llama_kill_degraded": "bogus"})
            check("kill_settings_bad_bool", False)
        except ValueError:
            check("kill_settings_bad_bool", True)
        p2 = settings_defaults()
        check("kill_settings_defaults",
              p2.get("llama_kill_degraded") is True
              and p2.get("llama_kill_tps") == 60.0
              and p2.get("llama_kill_degraded_s") == 3600)
    finally:
        coordinator.home_dir = old_home
        shutil.rmtree(tmp, ignore_errors=True)

    # escalation_events from a temp dir
    ed = Path(tempfile.mkdtemp())
    (ed / "escalation_20260905_114956_x.md").write_text("Worker asked for a decision.\nWhy: unclear scope.", encoding="utf-8")
    esc = escalation_events(str(ed))
    check("escalation_events", len(esc) == 1 and esc[0]["kind"] == "escalation"
          and "decision" in esc[0]["detail"])
    shutil.rmtree(ed, ignore_errors=True)

    # HTTP: ephemeral server, real endpoints, real files
    tmp = Path(tempfile.mkdtemp())
    old_home = coordinator.home_dir
    coordinator.home_dir = lambda: tmp
    (tmp / "state.json").write_text(json.dumps({"prev_head": "abc123", "continues": 2, "paused": False,
                                                "last_evaluated_mid": "msg_x", "last_poll_at": utcms()}),
                                    encoding="utf-8")
    (tmp / "config.json").write_text(json.dumps({"project_dir": "", "session_id": "ses",
                                                 "poll_minutes": 8}), encoding="utf-8")
    (tmp / "monitor.log").write_text("13:00:00 | no action: idle\n", encoding="utf-8")
    (tmp / "escalations").mkdir()
    (tmp / "escalations" / "esc_probe.md").write_text(
        "Worker asked for a decision.\nTest escalation file.", encoding="utf-8")
    (tmp / "permission_events.jsonl").write_text(
        json.dumps({"t": utcms(), "kind": "permission_accept", "id": "per_x", "ver": "v2",
                    "session": "ses_perm", "target": r"C:\Desktop\SomeApp\data"})
        + "\n", encoding="utf-8")
    server = make_server(0)
    port = server.server_address[1]
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()
    import urllib.request
    try:
        st = json.loads(urllib.request.urlopen("http://127.0.0.1:%d/api/state" % port, timeout=5).read())
        check("api_state", st.get("prev_head") == "abc123" and st.get("continues") == 2
              and st.get("monitor_online") is True and st.get("paused") is False)
        evs = json.loads(urllib.request.urlopen("http://127.0.0.1:%d/api/timeline" % port, timeout=5).read())
        check("api_escalation_in_timeline", any(e["kind"] == "escalation" for e in evs))
        check("api_permission_in_timeline",
              any(e["kind"] == "permission_accept" and "SomeApp" in e["detail"] for e in evs))
        evs_all = json.loads(urllib.request.urlopen(
            "http://127.0.0.1:%d/api/timeline?session=all" % port, timeout=5).read())
        check("api_timeline_all", isinstance(evs_all, list)
              and any(e["kind"] == "escalation" for e in evs_all)
              and any(e["kind"] == "permission_accept" for e in evs_all))
        req = urllib.request.Request("http://127.0.0.1:%d/api/control" % port,
                                     data=json.dumps({"intent": "pause"}).encode(),
                                     headers={"Content-Type": "application/json"})
        resp = json.loads(urllib.request.urlopen(req, timeout=5).read())
        check("api_control", resp.get("seq") == 1 and (tmp / "control.json").exists())
        tls = json.loads(urllib.request.urlopen("http://127.0.0.1:%d/api/timeline" % port, timeout=5).read())
        check("api_timeline_hydrated", isinstance(tls, list))
        req = urllib.request.Request("http://127.0.0.1:%d/api/session" % port,
                                     data=json.dumps({"session_id": "ses_x", "enabled": True}).encode(),
                                     headers={"Content-Type": "application/json"})
        sess = json.loads(urllib.request.urlopen(req, timeout=5).read())
        check("api_session_post", sess.get("ok") is True and sess.get("changed") is True
              and (coordinator.load_config().get("monitored_sessions") or {})
              .get("ses_x", {}).get("enabled") is True)
    finally:
        server.shutdown(); th.join(timeout=3)
        coordinator.home_dir = old_home
        shutil.rmtree(tmp, ignore_errors=True)

    # ntfy API: capped notify log + HTTP roundtrip
    tmp = Path(tempfile.mkdtemp())
    old_home = coordinator.home_dir
    coordinator.home_dir = lambda: tmp
    try:
        from opencode_hawk import notify
        cfg = coordinator.load_config()
        cfg.setdefault("notify", {})["ntfy"] = {
            "topic": "hawk-test", "server": "https://ntfy.sh",
            "priority": 3, "callback_base": "http://127.0.0.1:8765"}
        coordinator.save_json(tmp / "config.json", cfg)
        for i in range(notify.NOTIFY_LOG_CAP + 20):
            notify.append_notify_log({"ts": i, "dir": "sent", "kind": "test",
                                      "body": "full message %d" % i})
        lg = notify.load_notify_log()
        check("notify_log_capped",
              len(lg) == notify.NOTIFY_LOG_CAP
              and lg[0]["ts"] == 20
              and lg[-1]["ts"] == notify.NOTIFY_LOG_CAP + 19
              and lg[0]["body"] == "full message 20")
        server = make_server(0)
        port = server.server_address[1]
        th = threading.Thread(target=server.serve_forever, daemon=True)
        th.start()
        try:
            import urllib.request
            d = json.loads(urllib.request.urlopen(
                "http://127.0.0.1:%d/api/ntfy" % port, timeout=5).read())
            check("ntfy_api_get", d.get("topic") == "hawk-test"
                  and isinstance(d.get("log"), list)
                  and any(isinstance(e.get("body"), str) and e.get("body")
                          for e in d.get("log") or []))
            import urllib.error
            req3 = urllib.request.Request(
                "http://127.0.0.1:%d/api/ntfy" % port,
                data=json.dumps({"topic": "hawk-new"}).encode(),
                headers={"Content-Type": "application/json"})
            t3 = json.loads(urllib.request.urlopen(req3, timeout=5).read())
            check("ntfy_topic_update",
                  t3.get("ok") is True
                  and coordinator.load_config()["notify"]["ntfy"]["topic"] == "hawk-new")
            d2 = json.loads(urllib.request.urlopen(
                "http://127.0.0.1:%d/api/ntfy" % port, timeout=5).read())
            check("ntfy_topic_persisted", d2.get("topic") == "hawk-new")
            req4 = urllib.request.Request(
                "http://127.0.0.1:%d/api/ntfy" % port,
                data=json.dumps({"topic": "bad topic!"}).encode(),
                headers={"Content-Type": "application/json"})
            try:
                t4 = json.loads(urllib.request.urlopen(req4, timeout=5).read())
                err4 = str(t4.get("error") or "")
            except urllib.error.HTTPError as he:
                err4 = str(json.loads(he.read()).get("error") or "")
            check("ntfy_topic_validation", err4.startswith("topic must"))
            req5 = urllib.request.Request(
                "http://127.0.0.1:%d/api/ntfy" % port,
                data=json.dumps({}).encode(),
                headers={"Content-Type": "application/json"})
            try:
                t5 = json.loads(urllib.request.urlopen(req5, timeout=5).read())
                err5 = str(t5.get("error") or "")
            except urllib.error.HTTPError as he:
                err5 = str(json.loads(he.read()).get("error") or "")
            check("ntfy_topic_required", err5 == "topic required")
            cfg0 = coordinator.load_config()
            cfg0["notify"]["ntfy"]["topic"] = ""
            coordinator.save_json(tmp / "config.json", cfg0)
            req2 = urllib.request.Request(
                "http://127.0.0.1:%d/api/ntfy/test" % port,
                data=json.dumps({}).encode(),
                headers={"Content-Type": "application/json"})
            t2 = json.loads(urllib.request.urlopen(req2, timeout=5).read())
            check("ntfy_api_test_offline", t2.get("sent") is False)
            # ── ntfy poll: fake urlopen returns NDJSON; verify skip-self / FIFO ──
            import types
            cfg2 = coordinator.load_config()
            cfg2.setdefault("notify", {})["ntfy"] = {
                "topic": "hawk-test", "server": "https://ntfy.sh",
                "priority": 3, "callback_base": "http://127.0.0.1:8765"}
            coordinator.save_json(tmp / "config.json", cfg2)
            # Pre-populate map: self_id=abc, awaiting=["ses_999"], no cursor (first run)
            m = {"_ntfy_self": ["abc"], "_ntfy_awaiting": ["ses_999"]}
            coordinator.save_json(tmp / "pending_replies.json", m)
            # Fake NDJSON: keepalive + self message + user message + unrouted user message
            fake_ndjson = (
                '{"id":"k1","time":100,"event":"keepalive","topic":"hawk-test"}\n'
                '{"id":"abc","time":101,"event":"message","topic":"hawk-test","message":"self-msg"}\n'
                '{"id":"user1","time":102,"event":"message","topic":"hawk-test","message":"please continue"}\n'
                '{"id":"user2","time":103,"event":"message","topic":"hawk-test","message":"orphan reply"}\n'
            )
            captured = {}
            def _fake_apply(sid, text):
                captured["sid"] = sid
                captured["text"] = text
            _old_urlopen = urllib.request.urlopen
            class _FakeResponse:
                def __init__(self, data):
                    self._data = data
                    self.status = 200
                def read(self):
                    return self._data
                def __enter__(self):
                    return self
                def __exit__(self, *a):
                    pass
            def _fake_urlopen(req, timeout=0):
                return _FakeResponse(fake_ndjson.encode("utf-8"))
            urllib.request.urlopen = _fake_urlopen
            try:
                # Reset throttle so poll runs
                m2 = json.loads((tmp / "pending_replies.json").read_text(encoding="utf-8-sig"))
                m2["_ntfy_polled_at"] = 0
                tmp.joinpath("pending_replies.json").write_text(
                    json.dumps(m2), encoding="utf-8")
                n = notify.poll_ntfy_replies(_fake_apply)
                check("ntfy_poll_delivered", n == 1
                      and captured.get("sid") == "ses_999"
                      and captured.get("text") == "please continue")
                m3 = json.loads((tmp / "pending_replies.json").read_text(encoding="utf-8-sig"))
                check("ntfy_poll_cursor_advanced", m3.get("_ntfy_cursor") == "user2")
                check("ntfy_poll_awaiting_drained", m3.get("_ntfy_awaiting") == [])
                check("ntfy_poll_seen", "user1" in (m3.get("_ntfy_seen") or []))
            finally:
                urllib.request.urlopen = _old_urlopen
        finally:
            server.shutdown(); th.join(timeout=3)
    finally:
        coordinator.home_dir = old_home
        shutil.rmtree(tmp, ignore_errors=True)

    # sessions API: list ordering + settings upsert + selected
    tmpdb2 = Path(tempfile.mkdtemp()) / "opencode.db"
    con2 = sqlite3.connect(str(tmpdb2))
    con2.executescript(
        "CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, title TEXT, "
        "model TEXT, directory TEXT, time_created INTEGER, time_updated INTEGER);"
        "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, "
        "time_updated INTEGER, data TEXT);"
        "CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, "
        "time_created INTEGER, time_updated INTEGER, data TEXT);")
    con2.executemany(
        "INSERT INTO session VALUES (?,?,?,?,?,?,?)",
        [("s_op", None, "Open", '{"id":"big-pickle","providerID":"opencode"}',
          "C:/op", 3, 5),
         ("s_ll", None, "Llama", '{"id":"qwen3","providerID":"llama.cpp"}',
          "C:/ll", 4, 9),
         ("s_sub", "s_ll", "Sub", '{"id":"qwen3","providerID":"llama.cpp"}',
          "C:/ll", 6, 8)])
    con2.commit()
    con2.close()
    tmp = Path(tempfile.mkdtemp())
    old_home = coordinator.home_dir
    coordinator.home_dir = lambda: tmp
    orig_dbp = coordinator.db_path
    coordinator.db_path = lambda: tmpdb2
    try:
        coordinator.save_json(tmp / "config.json", {})
        lst = sessions_list_payload(show_all=True)
        ids = [e["id"] for e in lst]
        check("sessions_list_order", ids[0] == "s_ll" and "s_sub" in ids and "s_op" in ids)
        check("sessions_list_settings", lst[0]["settings"].get("enabled") is False)
        # subtask grouping: the child sits directly under its parent, ordered
        # newest-first; main sessions keep depth 0, children depth 1
        check("sessions_subtask_grouping", [e["id"] for e in lst] == ["s_ll", "s_sub", "s_op"]
              and next(e for e in lst if e["id"] == "s_ll")["depth"] == 0
              and next(e for e in lst if e["id"] == "s_sub")["depth"] == 1)
        # deeper chains nest recursively; children of a hidden parent stay
        # visible as depth-1 orphans at the end of the list
        con2b = sqlite3.connect(str(tmpdb2))
        con2b.execute("INSERT INTO session VALUES (?,?,?,?,?,?,?)",
                      ("s_gc", "s_sub", "Grandchild", '{"id":"qwen3","providerID":"llama.cpp"}',
                       "C:/ll", 5, 7))
        con2b.execute("INSERT INTO session VALUES (?,?,?,?,?,?,?)",
                      ("s_orph", "s_gone", "Orphan", '{"id":"qwen3","providerID":"llama.cpp"}',
                       "C:/ll", 4, 6))
        con2b.commit(); con2b.close()
        lst3 = sessions_list_payload(show_all=True)
        ids3 = [e["id"] for e in lst3]
        dep3 = {e["id"]: e["depth"] for e in lst3}
        check("sessions_subtask_nesting",
              ids3 == ["s_ll", "s_sub", "s_gc", "s_op", "s_orph"]
              and dep3["s_ll"] == 0 and dep3["s_sub"] == 1 and dep3["s_gc"] == 2
              and dep3["s_orph"] == 1)
        up = update_session_settings({"session_id": "s_ll", "enabled": True,
                                      "auto_continue": True})
        check("sessions_upsert", up["changed"] is True)
        lst2 = sessions_list_payload()
        e2 = next(e for e in lst2 if e["id"] == "s_ll")
        check("sessions_upsert_persisted", e2["settings"]["enabled"] is True
              and e2["settings"]["auto_continue"] is True)
        up2 = update_session_settings({"session_id": "s_ll", "selected": "s_ll"})
        cfg2 = coordinator.load_config()
        check("sessions_selected", cfg2.get("session_id") == "s_ll")
        try:
            update_session_settings({})
            check("sessions_requires_id", False)
        except ValueError:
            check("sessions_requires_id", True)
        up3 = update_session_settings({"session_id": "ghost", "remove": True})
        check("sessions_remove_noop", up3["changed"] is False)
        up4 = update_session_settings({"session_id": "s_ll", "remove": True})
        chk_cfg = coordinator.load_config()
        check("sessions_remove", up4["changed"] is True
              and "s_ll" not in (chk_cfg.get("monitored_sessions") or {}))
    finally:
        coordinator.db_path = orig_dbp
        coordinator.home_dir = old_home
        shutil.rmtree(tmp, ignore_errors=True)

    # sessions live filter: recent + enabled kept by default, ancient hidden
    # unless show_all; live flag surfaces per-session
    tmpdb3 = Path(tempfile.mkdtemp()) / "opencode.db"
    con3 = sqlite3.connect(str(tmpdb3))
    con3.executescript(
        "CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, title TEXT, "
        "model TEXT, directory TEXT, time_created INTEGER, time_updated INTEGER);")
    now_ms = utcms()
    day = 86_400_000
    con3.executemany(
        "INSERT INTO session VALUES (?,?,?,?,?,?,?)",
        [("f_recent", None, "Recent", '{"id":"qwen3","providerID":"llama.cpp"}',
          "C:/r", now_ms - 120_000, now_ms - 60_000),
         ("f_old", None, "Old", '{"id":"big-pickle","providerID":"opencode"}',
          "C:/o", now_ms - 8 * day, now_ms - 7 * day),
         ("f_en", None, "Enabled", '{"id":"qwen3","providerID":"llama.cpp"}',
          "C:/e", now_ms - 10 * day, now_ms - 9 * day)])
    con3.commit(); con3.close()
    tmpf = Path(tempfile.mkdtemp())
    old_home3 = coordinator.home_dir
    coordinator.home_dir = lambda: tmpf
    orig_dbp3 = coordinator.db_path
    coordinator.db_path = lambda: tmpdb3
    try:
        coordinator.save_json(tmpf / "config.json",
                              {"monitored_sessions": {"f_en": {"enabled": True}}})
        defl = {e["id"] for e in sessions_list_payload()}
        check("sessions_live_default", defl == {"f_recent", "f_en"})
        allids = {e["id"] for e in sessions_list_payload(show_all=True)}
        check("sessions_live_showall", allids == {"f_recent", "f_old", "f_en"})
        recent = next(e for e in sessions_list_payload() if e["id"] == "f_recent")
        en = next(e for e in sessions_list_payload() if e["id"] == "f_en")
        check("sessions_live_flags", recent["live"] is True and en["live"] is False)
    finally:
        coordinator.db_path = orig_dbp3
        coordinator.home_dir = old_home3
        shutil.rmtree(tmpf, ignore_errors=True)

    # recommended session: newest llama.cpp in project_dir with a message
    # wins; opencode sessions / other dirs lose; matches select_session auto-detect
    tmpdb4 = Path(tempfile.mkdtemp()) / "opencode.db"
    con4 = sqlite3.connect(str(tmpdb4))
    con4.executescript(
        "CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, title TEXT, "
        "model TEXT, directory TEXT, time_created INTEGER, time_updated INTEGER);"
        "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, "
        "time_updated INTEGER, data TEXT);")
    con4.executemany(
        "INSERT INTO session VALUES (?,?,?,?,?,?,?)",
        [("r_r1", None, "rec1", '{"id":"qwen3","providerID":"llama.cpp"}',
          "C:/pj", 200, 300),
         ("r_r2", None, "rec2", '{"id":"qwen3","providerID":"llama.cpp"}',
          "C:/pj", 250, 400),
         ("r_open", None, "op", '{"id":"big-pickle","providerID":"opencode"}',
          "C:/pj", 800, 900),
         ("r_other", None, "other", '{"id":"qwen3","providerID":"llama.cpp"}',
          "C:/other", 900, 999)])
    con4.execute("INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES ('m', 'r_r2', 1, 1, '{}')")
    con4.commit(); con4.close()
    tmpg = Path(tempfile.mkdtemp())
    old_home4 = coordinator.home_dir
    coordinator.home_dir = lambda: tmpg
    orig_dbp4 = coordinator.db_path
    coordinator.db_path = lambda: tmpdb4
    try:
        coordinator.save_json(tmpg / "config.json", {"project_dir": "C:\\pj"})
        con4b = sqlite3.connect(str(tmpdb4))
        rec = recommended_session_id(con4b, coordinator.load_config())
        con4b.close()
        check("sessions_recommended", rec == "r_r2")
        lst4 = {e["id"]: e["recommended"] for e in sessions_list_payload(show_all=True)}
        check("sessions_recommended_flag", lst4.get("r_r2") is True
              and lst4.get("r_open") is False and lst4.get("r_other") is False)
    finally:
        coordinator.db_path = orig_dbp4
        coordinator.home_dir = old_home4
        shutil.rmtree(tmpg, ignore_errors=True)

    # escalations list session filter: Session-line match, filename fallback, all
    edf = Path(tempfile.mkdtemp())
    old_home2 = coordinator.home_dir
    coordinator.home_dir = lambda: edf
    (edf / "escalations").mkdir()
    (edf / "escalations" / "escalation_20260906_100000_sesabc.md").write_text(
        "- Session: ses_abc (title)\nreason", encoding="utf-8")
    (edf / "escalations" / "escalation_20260906_100000_other.md").write_text(
        "- Session: ses_other (title)\nreason", encoding="utf-8")
    (edf / "escalations" / "escalation_20260906_100000_efghij.md").write_text(
        "no session line", encoding="utf-8")
    try:
        fa = [e["file"] for e in escalations_list("ses_abc")]
        check("escalations_filter", fa == ["escalation_20260906_100000_sesabc.md"])
        fz = [e["file"] for e in escalations_list("ses_efghij")]
        check("escalations_filter_fallback", fz == ["escalation_20260906_100000_efghij.md"])
        check("escalations_filter_none", len(escalations_list()) == 3)
    finally:
        coordinator.home_dir = old_home2
        shutil.rmtree(edf, ignore_errors=True)

    # telemetry: pure tps / compaction helpers (no network)
    check("telemetry_tps_ok", _tps({"wall_ms": 0, "decoded_tokens": 10},
                                   {"wall_ms": 1000, "decoded_tokens": 30}) == 20.0)
    check("telemetry_tps_zero_when_idle",
          _tps({"wall_ms": 0, "decoded_tokens": 10},
               {"wall_ms": 1000, "decoded_tokens": 10}) == 0.0)
    check("telemetry_tps_zero_on_reset",
          _tps({"wall_ms": 0, "decoded_tokens": 30},
               {"wall_ms": 1000, "decoded_tokens": 5}) == 0.0)
    check("telemetry_ptps_ok", _ptps({"wall_ms": 0, "processed_tokens": 0},
                                    {"wall_ms": 1000, "processed_tokens": 500}) == 500.0)
    check("telemetry_ptps_zero_when_idle",
          _ptps({"wall_ms": 0, "processed_tokens": 500},
                {"wall_ms": 1000, "processed_tokens": 500}) == 0.0)
    check("telemetry_ptps_zero_on_reset",
          _ptps({"wall_ms": 0, "processed_tokens": 500},
                {"wall_ms": 1000, "processed_tokens": 100}) == 0.0)
    check("telemetry_compact_drop",
          _compaction_detected({"prompt_tokens": 5000, "id_task": 1},
                               {"prompt_tokens": 900, "id_task": 1}))
    check("telemetry_compact_task_change",
          _compaction_detected({"prompt_tokens": 5000, "id_task": 1},
                               {"prompt_tokens": 400, "id_task": 2}))
    check("telemetry_no_compact_growth",
          not _compaction_detected({"prompt_tokens": 5000, "id_task": 1},
                                   {"prompt_tokens": 5100, "id_task": 1}))
    check("telemetry_no_compact_first", not _compaction_detected(None, {}))
    save_s = dash._tele_series
    save_m = dash._tele_meta
    try:
        dash._tele_series = [{"t": 1, "tokens": 2, "tps": 3.5, "compact": False,
                         "cpu_frac": 0.5, "ram_mb": 100.0, "vram_mb": 50.0,
                         "gpu_pct": 10.0}]
        dash._tele_meta = {"cpu_frac": 0.5, "ram_mb": 100.0, "vram_mb": 50.0,
                      "gpu_pct": 10.0, "processing": False, "n_ctx": 4096,
                      "pid": 1, "total_tokens": 12345}
        p = llama_telemetry_payload()
        pt = p["series"][0]
        check("telemetry_payload_fields",
              pt["cpu_frac"] == 0.5 and pt["gpu_pct"] == 10.0
              and pt["vram_mb"] == 50.0 and pt["ram_mb"] == 100.0
              and pt["tps"] == 3.5 and p["gpu_pct"] == 10.0
              and p["total_tokens"] == 12345)
        check("telemetry_payload_hw",
              "hw_sticks" in p and isinstance(p["hw_ram"], list)
              and all(s.get("locator") and s.get("gb", 0) > 0
                      for s in p["hw_ram"])
              and isinstance(p["hw_disks"], list)
              and all(d.get("label") and 0 <= d.get("pct", 0) <= 100
                      for d in p["hw_disks"])
              and isinstance(p["hw_gpus"], list)
              and all(g.get("name") and g.get("total_mb", 0) > 0
                      and g.get("used_mb", 0) >= 0
                      and 0 <= g.get("pct", 0) <= 100
                      for g in p["hw_gpus"]))
    finally:
        dash._tele_series = save_s
        dash._tele_meta = save_m

    # window parsing (pure) + totals resolvable on this box
    check("tele_window_parse", _window_seconds("5m") == 300
          and _window_seconds("15m") == 900 and _window_seconds("1h") == 3600
          and _window_seconds("6h") == 21600 and _window_seconds("24h") == 86400
          and _window_seconds("1d") == 86400 and _window_seconds("7d") == 604800
          and _window_seconds("30d") == 2592000
          and _window_seconds("bogus") == TELE_DEFAULT_WINDOW_S
          and _window_seconds(7200) == 7200)
    check("tele_totals_present",
          isinstance(coordinator.gpu_vram_total_mb(), float)
          and coordinator.gpu_vram_total_mb() > 0
          and coordinator.system_ram_total_mb() >= 0)
    # system-wide used readers: RAM used is always a float on windows; VRAM
    # used is a float or None (PDH adapter-memory counter may be absent)
    _ram_used = coordinator.system_ram_used_mb()
    _vram_used = coordinator.gpu_vram_used_mb()
    check("tele_used_present",
          (isinstance(_ram_used, float) and _ram_used >= 0)
          and (_vram_used is None or (isinstance(_vram_used, float)
                                      and _vram_used >= 0)))
    # hardware overview: stick count resolvable (or gracefully None), real
    # per-stick info, per-disk real-time activity and per-GPU VRAM all shaped
    # correctly when present, each cached as a copy
    check("hw_sticks_resolvable",
          hw_ram_sticks() is None or (isinstance(hw_ram_sticks(), int)
                                      and hw_ram_sticks() >= 1))
    _ram_info = hw_ram_sticks_info()
    check("hw_ram_info_shape",
          isinstance(_ram_info, list)
          and all(s.get("locator") and s.get("gb", 0) > 0
                  and s.get("speed", 0) >= 0 for s in _ram_info))
    _hw_disks = hw_disk_activity()
    check("hw_disks_shape",
          isinstance(_hw_disks, list)
          and all(isinstance(d, dict) and d.get("label")
                  and 0 <= d.get("pct", 0) <= 100 for d in _hw_disks))
    _hw_gpus = hw_gpus()
    check("hw_gpus_shape",
          isinstance(_hw_gpus, list)
          and all(isinstance(g, dict) and g.get("name")
                  and g.get("total_mb", 0) > 0   # real VRAM only
                  and g.get("used_mb", 0) >= 0
                  and 0 <= g.get("pct", 0) <= 100 for g in _hw_gpus))
    # live PDH feeds must not be vacuously empty on Windows: the per-adapter
    # engine read and per-disk activity read must each have real rows in range.
    # The background PDH sampler takes ~1 s to produce its first sample in a
    # fresh process, so wait briefly for it rather than racing it.
    if os.name == "nt":
        def _wait(reader, want=1, tries=8):
            val = []
            for _ in range(tries):
                val = reader()
                if len(val) >= want:
                    return val
                time.sleep(1.0)
            return val

        _eng = _wait(coordinator.gpu_adapters_engine_pct)
        _acc = _wait(coordinator.physical_disk_active_pct)
        _wg = _wait(hw_gpus)
        _wd = _wait(hw_disk_activity)
        check("pdh_engine_live",
              len(_eng) >= 1 and _eng[0][0].startswith("luid")
              and all(isinstance(p, (int, float)) and 0 <= p <= 100
                      for _, p in _eng))
        check("pdh_disk_live",
              len(_acc) >= 1 and _acc[0][0].isdigit()
              and all(0 <= p <= 100 for _, p in _acc))
        # HAWK_SKIP_LIVE_HW=1 (set in CI): hosted runners have no GPU.
        if not os.environ.get("HAWK_SKIP_LIVE_HW"):
            check("hw_gpus_live", len(_wg) >= 1)
        # every shown disk is a physical disk with >= 1 GB of storage: the
        # inventory maps each PDH index to its Win32_DiskDrive Size, so the
        # shown count must equal the count of >= 1 GB physical disks
        if _HW.get("disk_inv"):
            big = sum(1 for v in _HW["disk_inv"].values()
                      if (v.get("size") or 0) >= MIN_DISK_BYTES)
            check("hw_disks_size_filter", len(_wd) == big)
    # live-reading cache: second call inside the TTL window returns the same
    # objects and the cache is a copy (mutating the result must not leak in)
    # Take fresh baselines here: the live waits above can outlast the TTL on
    # a slow machine, so the earlier readings may already have expired.
    _d0, _g0 = hw_disk_activity(), hw_gpus()
    check("hw_disks_cache",
          hw_disk_activity() == _d0 and hw_disk_activity() is not _HW["disks"])
    check("hw_gpus_cache",
          hw_gpus() == _g0 and hw_gpus() is not _HW["gpus"])

    # SQLite store + bucket-averaged downsample (isolated temp DB)
    tmp = Path(tempfile.mkdtemp())
    old_home = coordinator.home_dir
    coordinator.home_dir = lambda: tmp
    try:
        base = utcms()
        for i in range(600):
            telemetry_record({"t": base - (600 - i) * 4000, "tokens": 100 + i,
                               "tps": 10.0, "ptps": 5.0, "compact": (i % 50 == 0),
                               "cpu_frac": 0.5, "cpu_pct": 50, "ram_mb": 100.0,
                               "vram_mb": 400.0, "gpu_pct": 20.0})
        h = telemetry_history(600 * 4 * 60, target=300)
        check("tele_history_downsample",
              h["raw"] == 600 and h["count"] == 300
              and h["points"][0]["t"] < h["points"][-1]["t"])
        check("tele_history_compact_or", any(p["compact"] for p in h["points"]))
        h2 = telemetry_history(600 * 4 * 60, target=2000)
        check("tele_history_no_ds_small", h2["count"] == 600 and h2["raw"] == 600)
        # token curves from opencode message accounting: subagent sessions
        # roll up into their parent, "all" equals the per-session curves
        # added together, cumulative from 0 at the window start.
        oc = sqlite3.connect(":memory:")
        oc.execute("CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT)")
        oc.execute("CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT,"
                   " time_created INTEGER, data TEXT)")
        oc.executemany("INSERT INTO session VALUES (?,?)",
                       [("sA", None), ("sA1", "sA"), ("sB", None)])
        nowt = 10_000_000

        def _msg(i, sid, done, tin, tout, role="assistant", reason=0):
            oc.execute("INSERT INTO message VALUES (?,?,?,?)", (
                "m%d" % i, sid, done - 5000, json.dumps({
                    "role": role, "time": {"completed": done},
                    "tokens": {"input": tin, "output": tout,
                               "reasoning": reason,
                               "cache": {"read": 99999}}})))
        _msg(1, "sA", nowt - 7_200_000, 5000, 500)    # before a 1h window
        _msg(2, "sA", nowt - 3_000_000, 100, 10)
        _msg(3, "sA1", nowt - 2_000_000, 200, 20, reason=5)
        _msg(4, "sB", nowt - 1_000_000, 400, 40)
        _msg(5, "sB", nowt - 500_000, 999, 999, role="user")
        ca = _token_curve(oc, "sA", 3600, nowt, buckets=7)
        cb = _token_curve(oc, "sB", 3600, nowt, buckets=7)
        call = _token_curve(oc, None, 3600, nowt, buckets=7)
        check("tok_curve_subagents_rollup",
              ca["total_input"] == 300 and ca["total_output"] == 35)
        check("tok_curve_window_axis",
              ca["points"][0]["t"] == nowt - 3_600_000
              and ca["points"][-1]["t"] == nowt and len(ca["points"]) == 7
              and ca["points"][0]["tokens_input"] == 0)
        check("tok_curve_monotonic",
              all(x["tokens_input"] <= y["tokens_input"]
                  for x, y in zip(ca["points"], ca["points"][1:])))
        check("tok_curve_all_is_sum",
              [p["tokens_input"] for p in call["points"]]
              == [x["tokens_input"] + y["tokens_input"]
                  for x, y in zip(ca["points"], cb["points"])]
              and call["total_input"] == 700)
        oc.close()
    finally:
        coordinator.home_dir = old_home
        shutil.rmtree(tmp, ignore_errors=True)

    # llama_restart: due-reason computation (pure)
    from opencode_hawk import llama_restart
    check("llama_restart_not_due",
          not llama_restart.due_reason(100000, 90000, 60, False)["due"])
    check("llama_restart_time_due",
          llama_restart.due_reason(100000, 90000, 5, False)["reasons"] == ["time"])
    check("llama_restart_degraded_due",
          llama_restart.due_reason(100000, 90000, 60, True)["reasons"] == ["degraded"])
    check("llama_restart_no_baseline",
          not llama_restart.due_reason(100000, None, 60, False)["due"])
    kcfg = {"llama_kill_degraded": True, "llama_kill_degraded_s": 300,
            "llama_kill_tps": 10.0}
    kd0 = llama_restart.kill_decision(
        {"llama_kill_degraded": False}, {"degraded": True, "avg_tps": 5.0},
        None, 100000)
    check("kill_decision_disabled",
          kd0["kill"] is False and kd0["state"] == "disabled"
          and kd0["degraded_since_ms"] is None)
    kd1 = llama_restart.kill_decision(
        kcfg, {"degraded": True, "avg_tps": 5.0}, None, 100000)
    check("kill_decision_wait",
          kd1["kill"] is False and kd1["state"] == "wait"
          and kd1["degraded_since_ms"] == 100000)
    kd2 = llama_restart.kill_decision(
        kcfg, {"degraded": True, "avg_tps": 5.0}, 100000, 100000 + 299 * 1000)
    check("kill_decision_not_yet",
          kd2["kill"] is False and kd2["state"] == "wait")
    kd3 = llama_restart.kill_decision(
        kcfg, {"degraded": True, "avg_tps": 5.0}, 100000, 100000 + 300 * 1000)
    check("kill_decision_armed",
          kd3["kill"] is True and kd3["state"] == "armed")
    kd4 = llama_restart.kill_decision(
        kcfg, {"degraded": True, "avg_tps": 12.0}, 100000, 100000 + 500 * 1000)
    check("kill_decision_tps_gate",
          kd4["kill"] is False and kd4["state"] == "clear"
          and kd4["degraded_since_ms"] is None)
    kd5 = llama_restart.kill_decision(
        kcfg, {"degraded": False, "avg_tps": 5.0}, 100000, 100000 + 500 * 1000)
    check("kill_decision_not_degraded",
          kd5["kill"] is False and kd5["state"] == "clear")

    # subagent push surfaces BOTH walls (update 2026-09-25): started rings at
    # launch, finished/error rings at release; the state-scoped eid keeps the
    # finish from being deduped by the start, and the watermark stays
    # monotonic so an idle sample re-pushes nothing.
    tmpdb6 = Path(tempfile.mkdtemp()) / "opencode.db"
    con6 = sqlite3.connect(str(tmpdb6))
    con6.executescript(
        "CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, title TEXT, "
        "model TEXT, directory TEXT, time_created INTEGER, time_updated INTEGER);"
        "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, "
        "time_updated INTEGER, data TEXT);"
        "CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, "
        "time_created INTEGER, time_updated INTEGER, data TEXT);")
    t6 = utcms() - 60000

    def seed_part6(status, start, end):
        con6.execute(
            "INSERT OR REPLACE INTO part VALUES (?,?,?,?,?,?)",
            ("prt_abc", "msg_x", "s_root", t6, t6,
             json.dumps({"type": "tool", "tool": "task",
                         "state": {"status": status,
                                   "time": {"start": start, "end": end},
                                   "input": {"subagent_type": "developer",
                                             "description": "push me"}}})))
        con6.commit()

    seed_part6("in_progress", t6, 0)
    tmp6 = Path(tempfile.mkdtemp())
    old_home6 = coordinator.home_dir
    coordinator.home_dir = lambda: tmp6
    orig_dbp6 = coordinator.db_path
    coordinator.db_path = lambda: tmpdb6
    from opencode_hawk import notify as notify6
    orig_npost = notify6._post_ntfy
    pushes6 = []

    def rec_post6(c, title, body, tags="", actions=None, kind="alert",
                  eid="", meta=None):
        pushes6.append({"title": title, "body": body, "kind": kind, "eid": eid})
        return True

    notify6._post_ntfy = rec_post6
    try:
        coordinator.save_json(
            tmp6 / "config.json",
            {"notify": {"ntfy": {"topic": "t", "kinds": ["subagent"]}},
             "session_id": "s_root", "project_dir": ""})
        _event_push_sample()
        check("push_started_surface",
              len(pushes6) == 1 and pushes6[0]["kind"] == "subagent"
              and pushes6[0]["eid"] == "subagent:prt_abc:started"
              and pushes6[0]["title"] == "[hawk] subagent developer: push me"
              and "started" in pushes6[0]["body"])
        check("push_started_watermark",
              notify6.get_watermark("home", "subagent") == t6)
        seed_part6("completed", t6, t6 + 5000)
        _event_push_sample()
        check("push_finished_surface",
              len(pushes6) == 2
              and pushes6[1]["eid"] == "subagent:prt_abc:finished"
              and "finished" in pushes6[1]["body"]
              and notify6.get_watermark("home", "subagent") == t6 + 5000)
        _event_push_sample()
        check("push_idle_no_repush", len(pushes6) == 2)
    finally:
        notify6._post_ntfy = orig_npost
        coordinator.db_path = orig_dbp6
        coordinator.home_dir = old_home6
        shutil.rmtree(tmp6, ignore_errors=True)
        shutil.rmtree(tmpdb6.parent, ignore_errors=True)

    # subagent START push waits for the name to load (bug: an unnamed start
    # rings "subagent subagent:" and, once the watermark passes it, never
    # re-rings with the real name). An unnamed start is dropped from `fresh`,
    # so the watermark stays behind it and the next poll retries; when the
    # name lands, the start rings with the real agent type.
    tmpdb8 = Path(tempfile.mkdtemp()) / "opencode.db"
    con8 = sqlite3.connect(str(tmpdb8))
    con8.executescript(
        "CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, title TEXT, "
        "model TEXT, directory TEXT, time_created INTEGER, time_updated INTEGER);"
        "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, "
        "time_updated INTEGER, data TEXT);"
        "CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, "
        "time_created INTEGER, time_updated INTEGER, data TEXT);")
    t8 = utcms() - 60000
    tmp8 = Path(tempfile.mkdtemp())
    old_home8 = coordinator.home_dir
    coordinator.home_dir = lambda: tmp8
    orig_dbp8 = coordinator.db_path
    coordinator.db_path = lambda: tmpdb8
    from opencode_hawk import notify as notify8
    orig_npost8 = notify8._post_ntfy
    pushes8 = []

    def rec_post8(c, title, body, tags="", actions=None, kind="alert",
                  eid="", meta=None):
        pushes8.append({"title": title, "body": body, "kind": kind, "eid": eid})
        return True

    notify8._post_ntfy = rec_post8
    try:
        coordinator.save_json(
            tmp8 / "config.json",
            {"notify": {"ntfy": {"topic": "t", "kinds": ["subagent"]}},
             "session_id": "s_root", "project_dir": ""})

        def seed_part8(subagent_type):
            con8.execute(
                "INSERT OR REPLACE INTO part VALUES (?,?,?,?,?,?)",
                ("prt_u", "msg_x", "s_root", t8, t8,
                 json.dumps({"type": "tool", "tool": "task",
                             "state": {"status": "in_progress",
                                       "time": {"start": t8, "end": 0},
                                       "input": ({"subagent_type": subagent_type,
                                                  "description": "loading"}
                                                 if subagent_type
                                                 else {"description": "loading"})}})))
            con8.commit()

        # No subagent_type yet -> the start must NOT ring, watermark stays set.
        seed_part8("")
        _event_push_sample()
        check("push_unnamed_start_held",
              len(pushes8) == 0
              and notify8.get_watermark("home", "subagent") in (None, 0))
        # Name lands on the next sample -> the start rings with the real name.
        seed_part8("reviewer")
        _event_push_sample()
        check("push_named_start_rings",
              len(pushes8) == 1 and pushes8[0]["kind"] == "subagent"
              and pushes8[0]["eid"] == "subagent:prt_u:started"
              and pushes8[0]["title"] == "[hawk] subagent reviewer: loading"
              and "started" in pushes8[0]["body"]
              and notify8.get_watermark("home", "subagent") == t8)
    finally:
        notify8._post_ntfy = orig_npost8
        coordinator.db_path = orig_dbp8
        coordinator.home_dir = old_home8
        shutil.rmtree(tmp8, ignore_errors=True)
        shutil.rmtree(tmpdb8.parent, ignore_errors=True)

    # _clock renders LOCAL wall time (bug: UTC was 2h behind on CEST hosts)
    _t7 = 1790350000000
    check("clock_is_local",
          _clock(_t7) == time.strftime("%H:%M:%S", time.localtime(_t7 / 1000)))

    # llama_restart: degradation backstop reads telemetry (pure DB, no network)
    tmpr = Path(tempfile.mkdtemp())
    old_home_r = coordinator.home_dir
    coordinator.home_dir = lambda: tmpr
    try:
        dbp = tmpr / "telemetry.db"
        con = sqlite3.connect(str(dbp))
        con.execute("CREATE TABLE telemetry (ts INTEGER, tps REAL, tokens INTEGER)")
        base = llama_restart._now_ms()
        # 6 active samples: slow + deep -> degraded
        for i in range(6):
            con.execute("INSERT INTO telemetry VALUES (?,?,?)",
                        (base - i * 1000, 12.0, 40000))
        # 4 recent idle/dropped samples must not count (tps <= 0 filtered)
        for i in range(4):
            con.execute("INSERT INTO telemetry VALUES (?,?,?)",
                        (base - (i * 1000 + 500), 0.0, 0))
        con.commit(); con.close()
        cfg_r = coordinator.load_config()
        deg = llama_restart.degraded_signal(cfg_r, db_path=dbp)
        check("llama_restart_degraded_true",
              deg["degraded"] and deg["active"] == 6
              and deg["avg_tps"] == 12.0 and deg["avg_depth"] == 40000)
        # healthy signature: fast samples -> not degraded
        con = sqlite3.connect(str(dbp))
        con.execute("DELETE FROM telemetry")
        for i in range(6):
            con.execute("INSERT INTO telemetry VALUES (?,?,?)",
                        (base - i * 1000, 18.0, 12000))
        con.commit(); con.close()
        deg2 = llama_restart.degraded_signal(cfg_r, db_path=dbp)
        check("llama_restart_degraded_false",
              not deg2["degraded"] and deg2["active"] == 6)
        # sparse samples: below min active -> not degraded
        con = sqlite3.connect(str(dbp))
        con.execute("DELETE FROM telemetry")
        con.execute("INSERT INTO telemetry VALUES (?,?,?)",
                    (base - 1000, 10.0, 50000))
        con.commit(); con.close()
        deg3 = llama_restart.degraded_signal(cfg_r, db_path=dbp)
        check("llama_restart_degraded_sparse",
              not deg3["degraded"] and deg3["active"] == 1)
    finally:
        coordinator.home_dir = old_home_r
        shutil.rmtree(tmpr, ignore_errors=True)

    # in-memory fallback when the DB is still empty (fresh start)
    tmpm = Path(tempfile.mkdtemp())
    old_home_m = coordinator.home_dir
    save_series = dash._tele_series
    coordinator.home_dir = lambda: tmpm
    try:
        base = utcms()
        dash._tele_series = [{"t": base - i * 4000, "tokens": i * 10, "tps": 1.0,
                         "ptps": 0.0, "compact": False, "cpu_frac": 0.1,
                         "cpu_pct": 10, "ram_mb": 50.0, "vram_mb": None,
                         "gpu_pct": 5.0} for i in range(20)]
        h = telemetry_history(10 * 60)
        check("tele_history_memfallback",
              h["raw"] == 20 and h["count"] == 20
              and h["points"][0]["t"] < h["points"][-1]["t"])
    finally:
        dash._tele_series = save_series
        coordinator.home_dir = old_home_m
        shutil.rmtree(tmpm, ignore_errors=True)

    # ── llama task activity engine (Task 1) ──────────────────────────────
    saved_tasks = dash._LLAMA_TASKS
    dash._LLAMA_TASKS = {}
    try:
        l1 = "308.35.258.222 I slot print_timing: id 0 | task 66655 | " \
             "n_gen = 226, tg = 11.90 t/s, tg_3s = 14.68 t/s"
        l2 = "309.15.890.870 I slot release: id 0 | task 66655 | " \
             "stop processing: n_tokens = 47595, truncated = 0"
        l3 = "309.44.464.040 I slot launch_slot_: id 0 | task 66975 | " \
             "processing task, is_child = 0"
        ev1, ev2, ev3 = (parse_llama_line(x) for x in (l1, l2, l3))
        check("llama_parse_timing", ev1 and ev1["kind"] == "print_timing"
              and ev1["task"] == 66655 and abs(ev1["tps"] - 11.90) < 0.01)
        check("llama_parse_release", ev2 and ev2["kind"] == "release"
              and ev2["tokens"] == 47595 and ev2["truncated"] == 0)
        check("llama_parse_launch", ev3 and ev3["kind"] == "launch_slot_"
              and ev3["task"] == 66975 and ev3["slot"] == 0)
        check("llama_parse_junk", parse_llama_line("kafka 7 I slot meh") is None)
        check("llama_tick_unit", _tick_to_ms("308", "35", "258", "222")
              == 308 * 60000 + 35 * 1000 + 258)
        # registry lifecycle: launch -> timing -> release (anchor 1_700_000_000)
        _llama_apply({"tick": 100, "kind": "launch_slot_", "task": 1, "slot": 0},
                     1_700_000_000)
        _llama_apply({"tick": 200, "kind": "print_timing", "task": 1,
                      "tps": 12.5}, 1_700_000_000)
        _llama_apply({"tick": 300, "kind": "release", "task": 1,
                      "tokens": 120, "truncated": 0}, 1_700_000_000)
        r1 = dash._LLAMA_TASKS.get(1) or {}
        check("llama_registry_lifecycle", r1.get("start") == 1_700_000_100
              and r1.get("end") == 1_700_000_300 and r1.get("status") == "released"
              and r1.get("tokens") == 120 and abs((r1.get("tps") or 0) - 12.5) < 0.01)
        # unknown task id (log resumed mid-task): release still lands
        _llama_apply({"tick": 400, "kind": "release", "task": 99,
                      "tokens": 7}, 1_700_000_000)
        r99 = dash._LLAMA_TASKS.get(99) or {}
        check("llama_registry_orphan", r99.get("status") == "released"
              and r99.get("end") == 1_700_000_400)
        # snapshot / restore round-trip
        snap = _llama_registry()
        dash._LLAMA_TASKS.clear()
        _llama_registry_restore(snap)
        check("llama_registry_roundtrip", (set(dash._LLAMA_TASKS) == {1, 99})
              and dash._LLAMA_TASKS[99]["end"] == 1_700_000_400)
        # stream health causes (pure)
        now = 1_800_000_000
        check("llama_stream_noserver", _llama_stream_cause(
            0, 1_700_000_000, True, now - 1, 360, now) == "no-server")
        check("llama_stream_noanchor", _llama_stream_cause(
            8332, 0, True, now - 1, 360, now) == "no-anchor")
        check("llama_stream_gap", _llama_stream_cause(
            8332, 1_700_000_000, True, now - 600_000, 360, now) == "gap")
        check("llama_stream_ok", _llama_stream_cause(
            8332, 1_700_000_000, True, now - 5_000, 360, now) == "")
    finally:
        dash._LLAMA_TASKS = saved_tasks

    # ── llama task correlation + subagent state machine (Task 2) ─────────
    saved_tasks = dash._LLAMA_TASKS
    dash._LLAMA_TASKS = {}
    mem = sqlite3.connect(":memory:")
    mem.executescript(
        "CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, "
        " time_created INT, time_updated INT, model TEXT);"
        "CREATE TABLE part (id TEXT PRIMARY KEY, session_id TEXT, "
        " time_created INT, time_updated INT, data TEXT);"
        "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, "
        " time_created INT, time_updated INT, data TEXT);"
    )
    try:
        mem.execute("INSERT INTO session VALUES "
                    "('root','',1000,9000,'{\"providerID\":\"llama.cpp\"}')")
        mem.execute("INSERT INTO session VALUES "
                    "('child','root',2000,8000,'{\"providerID\":\"llama.cpp\"}')")
        # subagent task part dl1: window [3000, 3500], child 'child'
        mem.execute("INSERT INTO part (id,session_id,time_created,time_updated,data)"
                    " VALUES ('dl1','root',2500,3500,"
                    "'{\"type\":\"tool\",\"tool\":\"task\",\"state\":{"
                    "\"status\":\"completed\",\"time\":{\"start\":3000,\"end\":3500},"
                    "\"metadata\":{\"sessionId\":\"child\"}}}')")
        now = 4000
        wins = _llama_owner_windows(mem, now)
        check("llama_owner_windows", {w["key"] for w in wins} ==
              {"part:dl1", "session:root", "session:child"}
              and next(w for w in wins if w["key"] == "part:dl1")["kind"] == "subagent")
        # serial assignment: task launched at 3100 (anchor 3000 + tick 100)
        # -> deepest active window (part:dl1, kind subagent beats session)
        _llama_apply({"tick": 100, "kind": "launch_slot_", "task": 7, "slot": 0},
                     3000)   # start wall = 3100
        _llama_assign_owners(mem, now)
        check("llama_assign_deepest", dash._LLAMA_TASKS[7]["owner"] == "part:dl1")
        # running task in flight -> started regardless of part status
        st = _llama_owner_state(mem, "part:dl1", "child", "running", 3000, now)
        check("llama_state_inflight", st[0] == "started")
        dash._LLAMA_TASKS = {}   # isolate the db-sourced checks from the assign case
        # state machine, error first
        st = _llama_owner_state(mem, "part:dl1", "child", "error", 3000, now)
        check("llama_state_error", st == ("finished", "error", "db"))
        # completed part -> finished (structural, no llama evidence)
        st = _llama_owner_state(mem, "part:dl1", "child", "completed", 3000, now)
        check("llama_state_completed", st == ("finished", "finished", "db"))
        # part still running, but the child's last assistant message completed
        mem.execute("INSERT INTO message "
                    "(id,session_id,time_created,time_updated,data)"
                    " VALUES ('ma','child',3100,3400,"
                    "'{\"role\":\"assistant\",\"time\":{\"created\":3100,"
                    "\"completed\":3400}}')")
        # ...which only ends ONE step: opencode still says running, so the
        # subagent stays started (bug: finished after its first turn)
        st = _llama_owner_state(mem, "part:dl1", "child", "running", 3000, now)
        check("llama_state_step_end_not_finished",
              st == ("started", "started", "db"))
        # ...unless the part is orphaned: no child activity for ORPHAN_MS
        st = _llama_owner_state(mem, "part:dl1", "child", "running", 3000,
                                3400 + LLAMA_ORPHAN_MS)
        check("llama_state_orphan_finished", st == ("finished", "finished", "db"))
        # ...but a FRESH stub (running/pending, no time.start yet, no child
        # data yet) is a just-launched subagent, not an orphan: anchor=0 is
        # 'no evidence', so it stays started (2026-10-02 bug: every new
        # task-tool part rang 'finished' at its creation second, 0 s).
        st = _llama_owner_state(mem, "part:new", "", "running", 0, now)
        check("llama_state_fresh_stub_started", st == ("started", "started", "db"))
        st = _llama_owner_state(mem, "part:new2", "", "pending", 0, now)
        check("llama_state_fresh_stub_pending_started",
              st == ("started", "started", "db"))
        # llama-sourced: owned task ended 6 s ago, part still running -> grace
        mem.execute("INSERT INTO session VALUES "
                    "('child2','root',5000,9000,'{\"providerID\":\"llama.cpp\"}')")
        dash._LLAMA_TASKS[8] = {"start": 10_000, "end": 12_000, "tps": 11.5,
                           "tokens": 90, "owner": "session:child2",
                           "kind": "session", "status": "released"}
        # session owners carry no part status -> tool-loop grace applies
        st = _llama_owner_state(mem, "session:child2", "", "", 5000, 18_000)
        check("llama_state_grace", st == ("finished", "finished", "llama"))
        # a task part opencode still calls running ignores the grace (a tool
        # running > 5 s is not the end of the subagent)
        dash._LLAMA_TASKS[9] = dict(dash._LLAMA_TASKS[8], owner="part:dl1")
        st = _llama_owner_state(mem, "part:dl1", "child", "running", 3000, 18_000)
        check("llama_state_running_part_no_grace", st[0] == "started")
        dash._LLAMA_TASKS.pop(9)
        # ZOMBIE part: structurally 'running' with no time.end, child frozen
        # at 3700 -> owner window capped at 3700 (independent evidence), not
        # `now`; a task started after the child froze is NOT absorbed by it.
        mem.execute("INSERT INTO session VALUES "
                    "('child3','root',3500,3700,'{\"providerID\":\"llama.cpp\"}')")
        mem.execute("INSERT INTO part (id,session_id,time_created,time_updated,data)"
                    " VALUES ('dz','root',3550,3700,"
                    "'{\"type\":\"tool\",\"tool\":\"task\",\"state\":{"
                    "\"status\":\"running\",\"time\":{\"start\":3600},"
                    "\"metadata\":{\"sessionId\":\"child3\"}}}')")
        mem.execute("INSERT INTO message "
                    "(id,session_id,time_created,time_updated,data)"
                    " VALUES ('mz','child3',3695,3700,"
                    "'{\"role\":\"assistant\",\"time\":{\"created\":3690,"
                    "\"completed\":3700}}')")
        now = 6000
        wz = {w["key"]: w for w in _llama_owner_windows(mem, now)}
        check("llama_zombie_window_capped",
              wz["part:dz"]["end"] == 3700 and wz["part:dz"]["end"] != now)
        dash._LLAMA_TASKS[9] = {"start": 3800, "end": 0, "tps": 0.0, "tokens": 0,
                           "owner": "", "kind": "", "status": "running"}
        _llama_assign_owners(mem, now)
        check("llama_zombie_not_owned",
              dash._LLAMA_TASKS[9]["owner"] != "part:dz"
              and dash._LLAMA_TASKS[9]["owner"] == "session:child")
        # an in-flight task owned by the zombie that STARTED BEFORE the cap
        # is real work: it keeps the window open; one started after the cap
        # is a misattribution and must NOT re-open the window
        dash._LLAMA_TASKS[10] = {"start": 3650, "end": 0, "tps": 0.0, "tokens": 0,
                            "owner": "part:dz", "kind": "subagent",
                            "status": "running"}
        wz2 = {w["key"]: w for w in _llama_owner_windows(mem, now)}
        check("llama_zombie_inflight_pre_cap_open",
              wz2["part:dz"]["end"] == now)
        dash._LLAMA_TASKS.pop(10, None)   # drop the legit in-flight task: it would
        dash._LLAMA_TASKS[11] = {"start": 3900, "end": 0, "tps": 0.0, "tokens": 0,
                            "owner": "part:dz", "kind": "subagent",
                            "status": "running"}
        wz3 = {w["key"]: w for w in _llama_owner_windows(mem, now)}
        check("llama_zombie_inflight_post_cap_still_capped",
              wz3["part:dz"]["end"] == 3700)
        # with the misattributed tasks gone the zombie event resolves its end
        # from the child's last activity (no end-before-start, no bogus now)
        for t9 in (9, 10, 11):
            dash._LLAMA_TASKS.pop(t9, None)
        evs = subagent_events(mem, "")
        sz = next((e for e in evs if e["mid"] == "dz"), None)
        check("llama_zombie_event_honest_end",
              sz is not None and sz["state"] == "finished"
              and sz["end"] == 3700 and sz["start"] == 3600
              and "tok" not in sz["detail"])
        # subagent_events emits the new keys and resolves child from metadata
        evs = subagent_events(mem, "")
        sa = next(e for e in evs if e["kind"] == "subagent")
        check("subagent_events_newkeys", sa["mid"] == "dl1"
              and sa["state"] in ("started", "finished")
              and sa["tag"] in ("started", "finished", "error")
              and sa["src"] in ("llama", "db")
              and sa["child"] == "child" and "tps" in sa)
    finally:
        mem.close()
        dash._LLAMA_TASKS = saved_tasks

    # --- block C: tailer era discipline (Task 4) ---
    # A concatenated multi-boot log must resolve to the newest boot so a
    # fresh dashboard never dates stale history with the current anchor,
    # and a rewind after respawn must land exactly at the first byte of the
    # re-read era (boot lines appended before last_at_ms commits).
    tmpd = tempfile.mkdtemp(prefix="hawk-era-")
    try:
        log = os.path.join(tmpd, "llama-server.out.log")
        lines = [
            "0.00.001.000 I slot launch_slot_: id  0 | task 1 | seq 1",
            "0.00.500.000 I slot print_timing: id  0 | task 1 | tg =  11.35 t/s",
            "0.10.289.833 I slot      release: id  0 | task 1 | n_tokens = 400,",
            "0.00.010.000 I slot launch_slot_: id  0 | task 2 | seq 1",
            "0.00.200.000 I slot      release: id  0 | task 2 | n_tokens = 12,",
        ]
        # newline="\n": text mode on Windows would otherwise write CRLF
        # and shift every byte offset (the CRLF case is tested below).
        with open(log, "w", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(lines) + "\n")
        b = _llama_boot_start(Path(log))
        expect = sum(len(l) + 1 for l in lines[:3])   # after task-1 era
        check("llama_era_boot_boundary", b == expect)
        with open(log, "rb") as f:                     # b is a BYTE offset
            f.seek(b)
            tail = f.read().decode("utf-8", "replace")
        evs = [parse_llama_line(ln) for ln in tail.splitlines()]
        evs = [ev for ev in evs if ev]
        check("llama_era_parse_newest",
              [ev["task"] for ev in evs] == [2, 2])
        anchor = 1_700_000_000_000
        walls = [anchor + ev["tick"] for ev in evs
                 if ev["kind"] == "launch_slot_"]
        check("llama_era_newest_walls", walls == [anchor + 10])
        with open(log, "wb") as f:                       # Windows console
            f.write(("\r\n".join(lines) + "\r\n").encode("utf-8"))
        bc = _llama_boot_start(Path(log))
        check("llama_era_boot_boundary_crlf",
              bc == sum(len(l) + 2 for l in lines[:3]))
        with open(log, "rb") as f:
            f.seek(bc)
            first = f.readline().decode("utf-8").strip()
        check("llama_era_crlf_lands_on_line", first == lines[3])
        with open(log, "w", encoding="utf-8", newline="\n") as f:
            f.write(lines[2] + "\n")
        check("llama_era_single_boot", _llama_boot_start(Path(log)) == 0)
        # capture health: idle + silent log is healthy; busy + frozen log
        # beyond the grace window is a capture lapse; a missing log is too
        _LLAMA_CAP.update(size=-1, since=0.0)
        check("llama_cap_idle_ok",
              _llama_capture_cause(100, False, 1000.0, 10) == ""
              and _llama_capture_cause(100, False, 5000.0, 10) == "")
        check("llama_cap_busy_growing_ok",
              _llama_capture_cause(200, True, 5001.0, 10) == ""
              and _llama_capture_cause(300, True, 5020.0, 10) == "")
        check("llama_cap_busy_frozen",
              _llama_capture_cause(300, True, 5025.0, 10) == ""
              and _llama_capture_cause(300, True, 5040.0, 10) == "no-capture")
        check("llama_cap_missing", _llama_capture_cause(-1, False, 1.0) == "no-capture")
        _LLAMA_CAP.update(size=-1, since=0.0, grew_at=0.0)
        # uncaptured: the log's last growth predates the current server's
        # spawn -> distinct honest cause (not the misleading no-capture
        # degradation); while the log is still growing it stays healthy
        check("llama_cap_uncaptured_grow",
              _llama_capture_cause(500, True, 6000.0, 10,
                                   server_start_s=9000.0, log_mtime=5900.0) == "")
        check("llama_cap_uncaptured",
              _llama_capture_cause(500, True, 6050.0, 10,
                                   server_start_s=9000.0, log_mtime=5900.0)
              == "uncaptured")
        # a server spawned AFTER the observed growth window is not flagged
        # (its writes would have moved grew_at forward)
        _LLAMA_CAP.update(size=-1, since=0.0, grew_at=0.0)
        check("llama_cap_captured_new_server",
              _llama_capture_cause(600, True, 7000.0, 10,
                                   server_start_s=7001.0, log_mtime=7050.0) == "")
        check("llama_cap_captured_new_server_frozen",
              _llama_capture_cause(600, True, 7060.0, 10,
                                   server_start_s=7001.0, log_mtime=7050.0)
              == "no-capture")
        _LLAMA_CAP.update(size=-1, since=0.0)
        # uncaptured -> ok: the llama IS healthy (busy); only its console
        # output was never captured (spawned outside hawk). That is not a
        # health degradation, so _llama_stream_health must report ok with a
        # hint, not 'degraded'. Force the uncaptured path by freezing the log
        # size, marking the server busy, and dating the last growth before the
        # server spawn.
        _LLAMA_CAP.update(size=1000, since=time.time() - 100, grew_at=8000.0)
        _tmpd = Path(tempfile.mkdtemp())
        (_tmpd / LLAMA_LOG_FN).write_text("x" * 1000)
        _saved = {}
        try:
            from opencode_hawk import llama_restart as _lr
            _saved = {
                "llama_pid": coordinator.llama_pid,
                "load_config": coordinator.load_config,
                "home_dir": coordinator.home_dir,
                "_process_spec": _lr._process_spec,
                "_llama_anchor_ms": vars(dash)["_llama_anchor_ms"],
                "llama_status": vars(dash)["llama_status"],
            }
            coordinator.llama_pid = lambda cfg: 424242
            coordinator.load_config = lambda: {"stall_llama_window_s": 360}
            coordinator.home_dir = lambda: _tmpd
            _lr._process_spec = lambda pid: {"started_ms": 9000000.0}
            vars(dash)["_llama_anchor_ms"] = lambda: 1000
            vars(dash)["llama_status"] = lambda: {"active": True}
            h = _llama_stream_health()
            check("stream_health_uncaptured_ok",
                  h["ok"] is True and h["cause"] == "uncaptured"
                  and bool(h.get("hint")))
        finally:
            coordinator.llama_pid = _saved["llama_pid"]
            coordinator.load_config = _saved["load_config"]
            coordinator.home_dir = _saved["home_dir"]
            _lr._process_spec = _saved["_process_spec"]
            vars(dash)["_llama_anchor_ms"] = _saved["_llama_anchor_ms"]
            vars(dash)["llama_status"] = _saved["llama_status"]
        _LLAMA_CAP.update(size=-1, since=0.0)
        # future-wall guard: an old-era line dated under a new anchor is
        # dropped, never stored; stale rows are purged when the table opens
        now = utcms()
        with _LLAMA_TASKS_LOCK:
            saved_reg = dict(dash._LLAMA_TASKS)
            dash._LLAMA_TASKS.clear()
        try:
            _llama_apply({"kind": "release", "task": 777, "tick": 3_600_000,
                          "tokens": 1}, now)
            with _LLAMA_TASKS_LOCK:
                check("llama_future_dropped", 777 not in dash._LLAMA_TASKS)
            _llama_registry_restore({"778": {"start": now + 3_600_000,
                                             "end": now + 3_600_000}})
            with _LLAMA_TASKS_LOCK:
                check("llama_future_restore_dropped", 778 not in dash._LLAMA_TASKS)
        finally:
            with _LLAMA_TASKS_LOCK:
                dash._LLAMA_TASKS.clear()
                dash._LLAMA_TASKS.update(saved_reg)
        import sqlite3 as _sq
        mem2 = _sq.connect(":memory:")
        try:
            mem2.execute("CREATE TABLE llama_tasks (task_id INTEGER, "
                         "start_wall INTEGER, end_wall INTEGER, tps REAL, "
                         "tokens INTEGER, owner TEXT, kind TEXT, status TEXT, "
                         "PRIMARY KEY (task_id, start_wall))")
            mem2.execute("INSERT INTO llama_tasks VALUES (1,?,?,0,0,'','','released')",
                          (now + 3_600_000, now + 3_600_000))
            mem2.execute("INSERT INTO llama_tasks VALUES (2,?,?,0,0,'','','released')",
                          (now - 1000, now - 500))
            _llama_tasks_table(mem2)
            ids = [r[0] for r in mem2.execute("SELECT task_id FROM llama_tasks")]
            check("llama_future_purged", ids == [2])
        finally:
            mem2.close()
    finally:
        shutil.rmtree(tmpd, ignore_errors=True)

    # ntfy delete helpers: engine stubbed, local-log sync verified
    from opencode_hawk import notify as _n
    orig_hm = coordinator.home_dir
    orig_cfg = coordinator.load_config
    orig_del = _n.delete_message
    orig_delall = _n.delete_all
    tmp2 = Path(tempfile.mkdtemp(prefix="ntfy-del-test-"))
    coordinator.home_dir = lambda: tmp2
    coordinator.load_config = lambda: {"notify": {"ntfy":
        {"topic": "t", "server": "https://ntfy.sh"}}}
    try:
        for e in [{"dir": "sent", "id": "a1", "title": "old", "ts": 1,
                   "topic": "t"},
                  {"dir": "sent", "id": "a2", "title": "new", "ts": 2,
                   "topic": "t"},
                  {"dir": "received", "kind": "reply", "ts": 3},
                  {"dir": "sent", "title": "no-id", "ts": 4}]:
            _n.append_notify_log(e)

        _n.delete_message = lambda cfg, mid: True
        r = ntfy_delete("a1")
        check("ntfy_delete_ok",
              r == {"ok": True, "deleted": True, "id": "a1"}
              and [e.get("id") for e in _n.load_notify_log()]
                  == ["a2", None, None])
        try:
            ntfy_delete("bad id with spaces")
            check("ntfy_delete_bad_id", False)
        except ValueError:
            check("ntfy_delete_bad_id", True)
        _n.delete_message = lambda cfg, mid: False
        try:
            ntfy_delete("a2")
            check("ntfy_delete_engine_fail", False)
        except RuntimeError:
            check("ntfy_delete_engine_fail", True)
        _n.delete_message = lambda cfg, mid: True
        _n.delete_all = lambda cfg: {"ok": True, "deleted": 2, "total": 2,
                                     "ok_ids": ["a1", "a2"]}
        r2 = ntfy_delete_all()
        check("ntfy_delete_all_local_sync",
              r2 == {"ok": True, "deleted": 2, "total": 2}
              and _n.load_notify_log() == [])
        # partial remote wipe: only confirmed-deleted ids prune local rows,
        # unconfirmed ids keep their local row (and their delete button)
        _n.append_notify_log({"dir": "sent", "id": "a3", "title": "partial",
                              "ts": 5, "topic": "t"})
        _n.append_notify_log({"dir": "sent", "id": "a4", "title": "unconfirmed",
                              "ts": 6, "topic": "t"})
        _n.delete_all = lambda cfg: {"ok": True, "deleted": 1, "total": 2,
                                     "ok_ids": ["a3"], "failed_ids": ["a4"]}
        r3 = ntfy_delete_all()
        check("ntfy_delete_all_partial_sync",
              r3 == {"ok": True, "deleted": 1, "total": 2}
              and [e.get("id") for e in _n.load_notify_log()] == ["a4"])
        # clear-all deletes only messages still live on the channel (not the
        # tombstoned ones, not local-only ids) and clears the local list.
        _n.append_notify_log({"dir": "sent", "id": "exp1", "title": "expired",
                              "ts": 6, "topic": "t"})
        _n.append_notify_log({"dir": "received", "id": "rr1", "kind": "reply",
                              "ts": 7})
        _n.delete_all = orig_delall
        _n.delete_message = lambda cfg, mid: True
        orig_rh = _n._ntfy_raw_history
        _n._ntfy_raw_history = lambda cfg: [
            {"event": "message", "id": "c1", "message": "x", "time": 1},
            {"event": "message", "id": "c2", "message": "y", "time": 2},
            {"event": "message_delete", "id": "t1", "sequence_id": "c2"}]
        try:
            r4 = _n.delete_all(coordinator.load_config())
            check("ntfy_clearall_live_only",
                  r4["total"] == 1 and r4["ok_ids"] == ["c1"]
                  and r4["failed_ids"] == [])
            r5 = ntfy_delete_all()
            check("ntfy_clearall_clears_list",
                  r5 == {"ok": True, "deleted": 1, "total": 1}
                  and _n.load_notify_log() == [])
        finally:
            _n._ntfy_raw_history = orig_rh
        # legacy backfill: sent entries without body recover their full body
        # from the ntfy cache by publish id; dlt- fallbacks, already-complete
        # and received rows stay untouched.
        _n.append_notify_log({"dir": "sent", "id": "b1", "title": "legacy",
                              "excerpt": "old snippet", "ts": 7, "topic": "t"})
        _n.append_notify_log({"dir": "sent", "id": "dlt-zzz", "title": "no-id",
                              "excerpt": "x", "ts": 8, "topic": "t"})
        _n.append_notify_log({"dir": "sent", "id": "orphan", "title": "stale",
                              "excerpt": "not in cache", "ts": 9, "topic": "t"})
        orig_ce = _n._ntfy_cached_events
        _n._ntfy_cached_events = lambda cfg: [
            {"id": "b1", "message": "full legacy body\nsecond line"},
            {"id": "unrelated", "message": "other"}]
        try:
            rb = _n.backfill_bodies(coordinator.load_config())
            lg = {e.get("id"): e for e in _n.load_notify_log()}
            check("ntfy_backfill_bodies",
                  rb.get("matched") == 1
                  and lg["b1"]["body"] == "full legacy body\nsecond line"
                  and lg["b1"]["backfilled"] is True
                  and "body" not in lg["dlt-zzz"]
                  and "body" not in lg["orphan"]
                  and "backfilled" not in lg["orphan"])
        finally:
            _n._ntfy_cached_events = orig_ce
        # cache-ids + prune-local: read-only cache enumeration mirror; local
        # prune of listed (already-gone) and id-less rows — no remote calls.
        _n._ntfy_cached_events = lambda cfg: [{"id": "b1", "message": "x"}]
        try:
            # channel state applies the feed like an ntfy client: deletes
            # remove, clears mark read, same-sequence messages replace
            raw_feed = [
                {"event": "message", "id": "b1", "time": 1000,
                 "title": "[hawk] T", "message": "first",
                 "tags": ["arrow_right"]},
                {"event": "message", "id": "b2", "time": 2000,
                 "title": "", "message": "second"},
                {"event": "message", "id": "b3", "time": 1500,
                 "title": "[hawk] gone", "message": "x"},
                {"event": "message_delete", "id": "d1", "sequence_id": "b3"},
                {"event": "message_clear", "id": "c1", "sequence_id": "b1"},
                {"event": "keepalive"},
            ]
            ch = _n.ntfy_channel_state(coordinator.load_config(),
                                       events=raw_feed)
            check("ntfy_channel_state",
                  [c["id"] for c in ch] == ["b2", "b1"]
                  and ch[0]["kind"] == "reply"
                  and ch[1]["kind"] == "commit"
                  and ch[1]["read"] is True
                  and ch[1]["title"] == "[hawk] T"
                  and ch[1]["message"] == "first"
                  and ch[1]["ts"] == 1_000_000)
            # a later message on the same sequence id replaces the earlier
            upd = raw_feed + [{"event": "message", "id": "u1",
                               "sequence_id": "b2", "time": 2100,
                               "message": "second v2"}]
            ch2 = _n.ntfy_channel_state(coordinator.load_config(),
                                        events=upd)
            check("ntfy_channel_state_update",
                  len(ch2) == 2 and ch2[0]["message"] == "second v2"
                  and ch2[0]["seq"] == "b2")
            check("ntfy_channel_state_no_topic",
                  _n.ntfy_channel_state({}, events=raw_feed) == [])
            check("ntfy_cache_ids_ok", ntfy_cache_ids() == {"ids": ["b1"]})
            rp1 = ntfy_prune_local({"ids": ["b1", "never-existed"]})
            lg = [e.get("id") for e in _n.load_notify_log()]
            check("ntfy_prune_local_listed",
                  rp1 == {"ok": True, "pruned": 1}
                  and lg == ["dlt-zzz", "orphan"])
            rp2 = ntfy_prune_local({"ids": []})
            check("ntfy_prune_local_empty",
                  rp2 == {"ok": True, "pruned": 0}
                  and [e.get("id") for e in _n.load_notify_log()]
                  == ["dlt-zzz", "orphan"])
            try:
                ntfy_prune_local({"ids": "not-a-list"})
                check("ntfy_prune_local_bad", False)
            except ValueError:
                check("ntfy_prune_local_bad", True)
            rp_all = ntfy_prune_local({"all": True})
            check("ntfy_prune_local_all",
                  rp_all == {"ok": True, "pruned": 2}
                  and not _n.load_notify_log())
        finally:
            _n._ntfy_cached_events = orig_ce
    finally:
        coordinator.home_dir = orig_hm
        coordinator.load_config = orig_cfg
        _n.delete_message = orig_del
        _n.delete_all = orig_delall
        shutil.rmtree(tmp2, ignore_errors=True)

    print()
    print("dashboard self-test: %s" % ("PASS" if ok else "FAIL"))
    return 1 if not ok else 0


if __name__ == "__main__":
    sys.exit(self_test())
