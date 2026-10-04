"""llama-server task activity engine: tails the llama-server console log, keeps a registry of task ids with start / end / speed, and attributes each task to the OpenCode session (or subagent part) that caused it.

Split out of dashboard.py and still part of its namespace: every dashboard-level name is used as
`dash.<name>` and dashboard re-binds the names defined here, so a patch on the dashboard module reaches
this code. Import it through dashboard, not directly."""
from __future__ import annotations
import json
import re
import threading
import time
from . import coordinator  # same dir; load_state/load_config/connect_db/db_path/git_head/...
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

from . import dashboard as dash  # shared namespace: see the module docstring


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
    m = dash._LLAMA_LINE_RE.match(line.rstrip("\r\n"))
    if not m:
        return None
    ev = {"tick": dash._tick_to_ms(*[m.group(i) for i in range(1, 5)]),
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
    with dash._LLAMA_TASKS_LOCK:
        return {k: dict(v) for k, v in dash._LLAMA_TASKS.items()}


def _llama_registry_restore(data) -> None:
    """Replace the registry from state.json (restart survival)."""
    with dash._LLAMA_TASKS_LOCK:
        dash._LLAMA_TASKS.clear()
        for k, v in (data or {}).items():
            try:
                if (dash._llama_is_future(int(v.get("start") or 0))
                        or dash._llama_is_future(int(v.get("end") or 0))):
                    continue          # stale future-dated entry: drop
                dash._LLAMA_TASKS[int(k)] = {
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
    with dash._LLAMA_TASKS_LOCK:
        return [dict(v) for v in dash._LLAMA_TASKS.values()
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
    return bool(wall) and wall > (now_ms or dash.utcms()) + dash.LLAMA_FUTURE_SLACK_MS


def _llama_apply(ev: dict, anchor: int) -> None:
    """Fold one parsed log event into the registry (idempotent, thread-safe).
    A release for an unseen task id (log resumed past its launch line) lands
    as an orphan: start = release wall, status released (fallback 4)."""
    wall = anchor + ev["tick"] if anchor else 0
    if dash._llama_is_future(wall):
        coordinator.log_debug("llama_task %s dropped: future wall %s"
                              % (ev.get("task"), wall))
        return
    tid = ev["task"]
    with dash._LLAMA_TASKS_LOCK:
        rec = dash._LLAMA_TASKS.get(tid)
        if rec is None:
            rec = {"start": 0, "end": 0, "tps": 0.0, "tokens": 0,
                   "owner": "", "kind": "", "status": "orphan"}
            dash._LLAMA_TASKS[tid] = rec
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
    cutoff = dash.utcms() + dash.LLAMA_FUTURE_SLACK_MS
    con.execute("DELETE FROM llama_tasks WHERE start_wall > ? OR end_wall > ?",
                (cutoff, cutoff))
    con.commit()


def _llama_record(tid: int, con) -> None:
    """Persist one task row (best-effort; called on release from the tailer
    and from the /api/tasks read path)."""
    try:
        with dash._LLAMA_TASKS_LOCK:
            rec = dict(dash._LLAMA_TASKS.get(tid) or {})
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
        snap = sorted(dash._llama_registry().items(),
                      key=lambda kv: kv[1].get("start") or 0)
        keep = [kv for kv in snap if kv[1].get("status") != "released"]
        keep += snap[-dash.LLAMA_TASKS_LIMIT:] if len(snap) > dash.LLAMA_TASKS_LIMIT else []
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
    anchor = dash._llama_anchor_ms()
    now_ms = dash.utcms()
    with dash._LLAMA_TASKS_LOCK:
        last_rel = max([rec.get("end") or 0
                        for rec in dash._LLAMA_TASKS.values()] or [0])
    busy = bool((dash.llama_status() or {}).get("active"))
    cause = dash._llama_stream_cause(pid, anchor, busy, last_rel,
                                int(cfg.get("stall_llama_window_s", 360)),
                                now_ms)
    out = {"ok": not cause, "cause": cause or "ok"}
    if not cause and pid:
        # Capture lapse = llama BUSY while the console log stops growing.
        # Idle llama writes nothing, so silence alone is healthy. Growth is
        # judged by file SIZE: on Windows the mtime of a file held open for
        # append by llama-server is updated lazily and cannot be trusted.
        try:
            log_stat = (coordinator.home_dir() / dash.LLAMA_LOG_FN).stat()
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
        cause = dash._llama_capture_cause(size, busy, time.time(),
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
    grace = grace_s or 4 * dash.LLAMA_TAIL_INTERVAL_S
    if size != dash._LLAMA_CAP["size"] or not busy:
        if size != dash._LLAMA_CAP["size"] and dash._LLAMA_CAP["size"] < 0:
            # First sight after a loss: can't date growth, seed from the file
            # mtime (lazy/coarse on Windows, but the margin is wide and a
            # stale seed only biases toward the honest 'uncaptured' label).
            if log_mtime > 0:
                dash._LLAMA_CAP["grew_at"] = log_mtime
        elif size != dash._LLAMA_CAP["size"]:
            dash._LLAMA_CAP["grew_at"] = now_s
        dash._LLAMA_CAP["size"] = size
        dash._LLAMA_CAP["since"] = now_s
        return ""
    if now_s - dash._LLAMA_CAP["since"] <= grace:
        return ""
    if server_start_s and dash._LLAMA_CAP["grew_at"] and \
            dash._LLAMA_CAP["grew_at"] + dash.UNCAPTURED_MARGIN_S < server_start_s:
        return "uncaptured"
    return "no-capture"


def _llama_tick_ms(line: str):
    """Leading A.B.C(.D) prefix of ANY console line -> ms since boot start
    (same unit as _tick_to_ms), or None for lines without the prefix."""
    m = dash._TICK_RE.match(line)
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
        t = dash._llama_tick_ms(chunk.decode("utf-8", "replace"))
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
    log = coordinator.home_dir() / dash.LLAMA_LOG_FN
    while True:
        try:
            anchor = dash._llama_anchor_ms()
            if anchor:
                if dash._LLAMA_ANCHOR_MS == 0:
                    dash._LLAMA_ANCHOR_MS = anchor
                    dash._LLAMA_TAIL_POS = dash._llama_boot_start(log)
                    dash._LLAMA_ANCHOR_POS = dash._LLAMA_TAIL_POS
                elif anchor != dash._LLAMA_ANCHOR_MS:
                    dash._LLAMA_ANCHOR_MS = anchor
                    # Rewind to the NEWEST boot boundary, not merely to the
                    # start of the last read: that read can also hold the
                    # previous process's final lines, whose large uptime
                    # ticks would be dated under the new anchor -> future
                    # walls. Fall back to the last read start if no boundary
                    # is visible (single boot in the file).
                    boot = dash._llama_boot_start(log)
                    dash._LLAMA_TAIL_POS = boot if boot else dash._LLAMA_ANCHOR_POS
            try:
                size = log.stat().st_size if log.exists() else 0
            except OSError:
                size = 0
            if size < dash._LLAMA_TAIL_POS:
                dash._LLAMA_TAIL_POS = 0        # file truncated/replaced
                dash._LLAMA_ANCHOR_POS = 0
            if size > dash._LLAMA_TAIL_POS and dash._LLAMA_ANCHOR_MS:
                dash._LLAMA_ANCHOR_POS = dash._LLAMA_TAIL_POS   # era starts at this read
                with open(log, "r", encoding="utf-8", errors="replace") as f:
                    f.seek(dash._LLAMA_TAIL_POS)
                    fresh = f.read(size - dash._LLAMA_TAIL_POS).splitlines()
                dash._LLAMA_TAIL_POS = size
                released = []
                for ln in fresh:
                    ev = dash.parse_llama_line(ln)
                    if ev:
                        dash._llama_apply(ev, dash._LLAMA_ANCHOR_MS)
                        if ev["kind"] == "release":
                            released.append(ev["task"])
                if released:
                    # Owners come from opencode.db structure; llama_tasks
                    # history lives in hawk_telemetry.db (_tele_db), kept in
                    # sync here - the table's only write path.
                    con = None
                    try:
                        con = coordinator.connect_db(coordinator.db_path())
                        dash._llama_assign_owners(con, dash.utcms())
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
                            dash._llama_record(tid, tcon)
                    finally:
                        tcon.close()
                    dash._llama_save_persisted()
        except Exception as e:
            coordinator.log_debug("llama_tailer: %s" % e)
        time.sleep(dash.LLAMA_TAIL_INTERVAL_S)


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
                cap = dash._llama_child_last_activity(con, child) if child else 0
                if cap:
                    live = [r for r in dash._llama_owned_tasks("part:" + pid)
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
    wins = dash._llama_owner_windows(con, now_ms)
    if not wins:
        return
    with dash._LLAMA_TASKS_LOCK:
        for tid, rec in dash._LLAMA_TASKS.items():
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
    owned = dash._llama_owned_tasks(owner)
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
        last = dash._llama_child_last_activity(con, child) if child else 0
        anchor = max(last, part_start or 0)
        if anchor and now_ms - anchor >= dash.LLAMA_ORPHAN_MS:
            return "finished", "finished", src
        return "started", "started", src
    if child and dash._llama_child_terminal(con, child, part_start or 0):
        return "finished", "finished", src
    last_end = max([t.get("end") or 0 for t in owned] or [0])
    if last_end and now_ms - last_end >= dash.LLAMA_GRACE_MS:
        return "finished", "finished", src
    return "started", "started", src
