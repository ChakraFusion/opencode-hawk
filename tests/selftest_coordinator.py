#!/usr/bin/env python3
"""Coordinator self-test: synthetic rule-engine scenarios, no network.

Run directly (`python tests/selftest_coordinator.py`) or through
`python coordinator.py --self-test`.

The scenarios were written inside coordinator.py and use its names
(including private helpers) unqualified, so the module's namespace is
mirrored here. Patches that must be seen by coordinator code go through
`vars(hawk)`, the coordinator module's real namespace.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from opencode_hawk import coordinator as hawk  # noqa: E402

globals().update({k: v for k, v in vars(hawk).items()
                  if not (k.startswith("__") and k.endswith("__"))})


def self_test():
    """Hermetic wrapper: scenarios must not depend on whether a real
    llama-server happens to be running on this machine. The liveness probe
    (tasklist) is stubbed to "alive"; the llama_down scenarios override it
    explicitly. The real probe is restored afterwards."""
    g = vars(hawk)
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
        g = vars(hawk)
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
        g = vars(hawk)
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

    # T: session tree parent_id leaf resolution
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
            ok_t = leaf["id"] == "child"
    finally:
        db_path.__globals__["db_path"] = orig_db
        shutil.rmtree(tmpdb.parent, ignore_errors=True)
    results.append(("multi_session_tree", "pass", "pass", ok_t,
                    "parent_id leaf resolution", []))
    print("%-24s expect=%-8s got=%-8s %s  %s" % (
        "multi_session_tree", "pass", "pass",
        "OK" if ok_t else "MISMATCH",
        "parent_id leaf resolution"))

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


if __name__ == "__main__":
    sys.exit(self_test())
