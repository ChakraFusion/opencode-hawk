"""Dashboard telemetry: the llama-server sampler (context size, generation speed, CPU / RAM / VRAM / GPU), the hardware overview gauges (RAM sticks, per-GPU VRAM, disk activity) and the SQLite telemetry store with server-side downsampling.

Split out of dashboard.py and still part of its namespace: every dashboard-level name is used as
`dash.<name>` and dashboard re-binds the names defined here, so a patch on the dashboard module reaches
this code. Import it through dashboard, not directly."""
from __future__ import annotations
import base64
import json
import os
import sqlite3
import subprocess
import threading
import time
from pathlib import Path
from . import coordinator  # same dir; load_state/load_config/connect_db/db_path/git_head/...
from . import hawk_linux

from . import dashboard as dash  # shared namespace: see the module docstring


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
    now = time.time()
    if dash._VRAM_CACHE and now - dash._VRAM_CACHE[0] < 30 and dash._VRAM_CACHE[1] == pid:
        return dash._VRAM_CACHE[2]
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
    dash._VRAM_CACHE = (now, pid, mb)
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
    cfg = coordinator.load_config()
    pid = coordinator.llama_pid(cfg)
    slots = coordinator.llama_slots(cfg) if pid else None
    ls = dash.llama_status()
    now_ms = dash.utcms()
    cur = dict(slots) if slots else None
    if cur is not None:
        cur["wall_ms"] = now_ms
        dec = dash._slots_decoded(cfg)
        if dec is not None:
            cur["decoded_tokens"] = dec
    with dash._tele_lock:
        prev = dash._tele_series[-1] if dash._tele_series else None
        tps = 0.0
        ptps = 0.0
        if cur:
            if dash._tele_last_raw is not None:
                tps = dash._tps(dash._tele_last_raw, cur)
                ptps = dash._ptps(dash._tele_last_raw, cur)
            dash._tele_last_raw = cur
        elif dash._tele_last_raw is not None:
            dash._tele_last_raw = None  # llama went idle; next busy sample starts fresh
        compact = dash._compaction_detected(prev, cur) if cur and prev else False
        comp_from = prev["tokens"] if prev else 0
        tokens = cur["prompt_tokens"] if cur else (prev["tokens"] if prev else 0)
        # Running count of tokens processed: on the first busy sample the total
        # is seeded with the current context (a dashboard restart mid-session
        # keeps the count from being 0); afterwards add context growth and
        # decoded growth since the last sample, clamped so a context reset
        # (compaction, new task) never subtracts.
        prev_ctx = dash._tele_meta.get("ctx_tokens")
        total_add = 0
        if cur and prev_ctx is None:
            total_add = tokens
        elif prev_ctx is not None and tokens > prev_ctx:
            total_add = tokens - prev_ctx
        dash._tele_meta["ctx_tokens"] = tokens
        if cur is not None and cur.get("decoded_tokens") is not None:
            dec = cur["decoded_tokens"]
            prev_dec = dash._tele_meta.get("decoded_tokens")
            if prev_dec is not None and dec > prev_dec:
                total_add += dec - prev_dec
            dash._tele_meta["decoded_tokens"] = dec
        elif not cur:
            dash._tele_meta.pop("decoded_tokens", None)
        dash._tele_meta["total_tokens"] = dash._tele_meta.get("total_tokens", 0) + total_add
        cpu_frac = ls.get("cpu_frac") or 0.0
        cpu_pct = dash._cpu_pct(cpu_frac)
        dash._tele_meta["cpu_frac"] = cpu_frac
        dash._tele_meta["cpu_pct"] = cpu_pct
        dash._tele_meta["pid"] = pid
        if cur:
            dash._tele_meta["n_ctx"] = cur.get("n_ctx") or 0
            dash._tele_meta["processing"] = bool(cur.get("is_processing"))
        if not cur:
            dash._tele_meta["processing"] = False
        # System-wide RAM/VRAM in use (Task Manager style). Fall back to the
        # llama process' own numbers only if the system read is unavailable.
        ram_mb = coordinator.system_ram_used_mb()
        if ram_mb is None:
            memio = coordinator.llama_mem_io(pid) if pid else None
            if memio:
                ram_mb = round(memio[0] / 1024.0 / 1024.0, 1)
        dash._tele_meta["ram_mb"] = ram_mb
        vram = coordinator.gpu_vram_used_mb()
        if vram is None:
            vram = dash.llama_vram_mb(pid)
        dash._tele_meta["vram_mb"] = vram
        # WDDM's GPU Engine counter does not attribute AMD ROCm/HIP compute to
        # the llama process (per-pid reads are 0 even mid-generation), so use
        # the AHK-style system-wide max engine % — the same signal the user's
        # telemetry_monitor_fast.ahk dashboard showed.
        gpu_pct = dash.gpu_system_max()
        dash._tele_meta["gpu_pct"] = gpu_pct
        dash._tele_series.append({"t": now_ms, "tokens": tokens,
                              "tps": round(tps, 2), "ptps": round(ptps, 2),
                              "compact": bool(compact),
                              "cpu_frac": round(cpu_frac, 3), "cpu_pct": cpu_pct,
                              "ram_mb": ram_mb, "vram_mb": vram,
                              "gpu_pct": gpu_pct})
        if compact:
            dash._tele_compactions.append({"t": now_ms, "from": comp_from, "to": tokens})
        if len(dash._tele_series) > dash.MAX_SERIES:
            dash._tele_series = dash._tele_series[-dash.MAX_SERIES:]
        if len(dash._tele_compactions) > 100:
            dash._tele_compactions = dash._tele_compactions[-100:]
        pt = dash._tele_series[-1]
    try:
        dash.telemetry_record(pt)
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
                    evs = dash.git_log_events(pd, 5)
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
                since = dash.utcms() - 2 * 86400 * 1000
                for kind, fn in (("milestone", lambda c: dash.milestone_events(c, sid)),
                                 ("subagent", lambda c: dash.subagent_events(c, "", since))):
                    evs = fn(con)
                    if not evs:
                        continue
                    wt = int(notify.get_watermark("home", kind) or 0)
                    # Subagent finishes are detected with a lag, so one can
                    # land behind a newer start that already moved the
                    # watermark; look back and let the eid dedupe decide.
                    lb = dash.SUBAGENT_PUSH_LOOKBACK_MS if kind == "subagent" else 0
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
                                body = e.get("body") or e.get("detail") or title
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
                title = "permission %s: %s" % (verb, dash.short(target.split(",")[0], 60))
                if sess:
                    title += " · %s" % dash.short(sess, 12)
                notify.record_event(
                    cfg, "permission", title, (target or title)[:220],
                    eid="perm:%s:%s" % (k, str(d.get("id") or "")),
                    meta={"session": sess, "kind": k})
    except Exception as e:
        coordinator.log_debug("event_push: %s" % e)


def _tele_loop():
    dash._tele_restore_state()
    while True:
        try:
            dash._tele_sample()
        except Exception as e:
            coordinator.log_debug("telemetry: %s" % e)
        try:
            dash._event_push_sample()
        except Exception as e:
            coordinator.log_debug("event_push: %s" % e)
        time.sleep(dash.TELE_INTERVAL_S)


def llama_telemetry_payload() -> dict:
    """Snapshot for /api/llama: live resource tiles + the sampled series
    (context tokens / t/s / resource samples / compaction markers) for the
    charts. vram_total_mb / ram_total_mb pin the chart Y-axes and the
    used/total/% tiles."""
    vram_total = coordinator.gpu_vram_total_mb()
    ram_total = coordinator.system_ram_total_mb()
    with dash._tele_lock:
        last = dash._tele_series[-1] if dash._tele_series else {}
        # Values that are also charted come from the latest series point so the
        # text tiles and the graphs always display identical numbers; only
        # non-charted state (pid / n_ctx / processing / disk rates) comes from
        # _tele_meta.
        vram_mb = last.get("vram_mb", dash._tele_meta.get("vram_mb"))
        ram_mb = last.get("ram_mb", dash._tele_meta.get("ram_mb"))
        return {
            "pid": dash._tele_meta.get("pid"),
            "n_ctx": dash._tele_meta.get("n_ctx", 0),
            "processing": bool(dash._tele_meta.get("processing")),
            "cpu_frac": last.get("cpu_frac", dash._tele_meta.get("cpu_frac", 0.0)),
            "cpu_pct": last.get("cpu_pct", dash._tele_meta.get("cpu_pct", 0)),
            "ram_mb": ram_mb,
            "vram_mb": vram_mb,
            "gpu_pct": last.get("gpu_pct", dash._tele_meta.get("gpu_pct", 0.0)),
            "tps": last.get("tps", 0.0),
            "ptps": last.get("ptps", 0.0),
            "context_tokens": last.get("tokens", 0),
            "total_tokens": dash._tele_meta.get("total_tokens", 0),
            "vram_total_mb": vram_total,
            "ram_total_mb": ram_total,
            "vram_pct": round(100.0 * vram_mb / vram_total, 1) if (vram_mb and vram_total) else 0,
            "ram_pct": round(100.0 * ram_mb / ram_total, 1) if (ram_mb and ram_total) else 0,
            "hw_sticks": dash.hw_ram_sticks(),
            "hw_ram": dash.hw_ram_sticks_info(),
            "hw_disks": dash.hw_disk_activity(),
            "hw_gpus": dash.hw_gpus(),
            "series": list(dash._tele_series),
            "compactions": list(dash._tele_compactions),
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
    if dash._HW.get("sticks") is None:
        with dash._HW_LOCK:
            if dash._HW.get("sticks") is None:
                dash._HW["sticks"], dash._HW["sticks_info"] = dash._hw_sticks_sync()
    return dash._HW["sticks"], dash._HW["sticks_info"]


def hw_ram_sticks() -> int | None:
    """Physically installed RAM stick count (static hardware, cached)."""
    return dash._hw_sticks()[0]


def hw_ram_sticks_info() -> list[dict]:
    """[{locator, gb, speed}] real per-stick data; [] when the query failed."""
    return [dict(s) for s in dash._hw_sticks()[1]]


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
    if dash._HW.get("gpu_totals") is None:
        with dash._HW_LOCK:
            if dash._HW.get("gpu_totals") is None:
                dash._HW["gpu_totals"] = dash._hw_gpu_totals_sync()
    eng = coordinator.gpu_adapters_engine_pct()
    inst = coordinator.gpu_adapters_usage_mb()
    now = time.time()
    with dash._HW_LOCK:
        if dash._HW.get("gpus_at", 0.0) + dash.HW_TTL_S > now:
            return [dict(g) for g in dash._HW["gpus"]]
        totals = dash._HW.get("gpu_totals") or []
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
        prev = dash._HW.get("gpus")
        if not out and prev:
            return [dict(g) for g in prev]
        if out:
            dash._HW["gpus"] = out
            dash._HW["gpus_at"] = now
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
    if dash._HW.get("disk_inv") is None:
        with dash._HW_LOCK:
            if dash._HW.get("disk_inv") is None:
                dash._HW["disk_inv"] = dash._hw_disk_inventory_sync()
    inst = coordinator.physical_disk_active_pct()
    now = time.time()
    with dash._HW_LOCK:
        if dash._HW.get("disks_at", 0.0) + dash.HW_TTL_S > now:
            return [dict(d) for d in dash._HW["disks"]]
        inv = dash._HW["disk_inv"]
        out = []
        for idx, pct in inst:
            info = inv.get(idx) or {}
            if info and (info.get("size") or 0) < dash.MIN_DISK_BYTES:
                continue  # <1 GB storage: card reader / sub-GB stick, not real
            letters = info.get("letters") or []
            label = ", ".join(letters) if letters else ("Disk " + idx)
            out.append({"label": label, "pct": pct})
        # never cache a cold-start empty (background PDH sampler warms up
        # ~1 s): hold the last good snapshot, else return [] uncached
        prev = dash._HW.get("disks")
        if not out and prev:
            return [dict(d) for d in prev]
        if out:
            dash._HW["disks"] = out
            dash._HW["disks_at"] = now
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
    return dash.TELE_WINDOWS.get(str(w).strip().lower(), dash.TELE_DEFAULT_WINDOW_S)


def _tele_db_path() -> Path:
    return coordinator.home_dir() / dash.TELE_DB_NAME


def _tele_db():
    con = sqlite3.connect(str(dash._tele_db_path()), timeout=5)
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
    dash._llama_tasks_table(con)
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
    con = dash._tele_db()
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
              dash._tele_meta.get("pid"),
              dash._tele_meta.get("total_tokens", 0),
              dash._tele_meta.get("ctx_tokens", 0),
              dash._tele_meta.get("decoded_tokens", 0),
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
        con = dash._tele_db()
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
    dash._tele_backfill_totals()
    try:
        con = dash._tele_db()
        try:
            r = con.execute(
                "SELECT total_tokens, ctx_tokens, decoded_tokens FROM telemetry "
                "ORDER BY ts DESC LIMIT 1").fetchone()
        finally:
            con.close()
        if r and r[0] is not None and r[1] is not None:
            dash._tele_meta["total_tokens"] = int(r[0])
            dash._tele_meta["ctx_tokens"] = int(r[1])
            if r[2] is not None:
                dash._tele_meta["decoded_tokens"] = int(r[2])
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
    now = time.time()
    if now - dash._tele_purge_at < 120:
        return
    dash._tele_purge_at = now
    try:
        con.execute("DELETE FROM telemetry WHERE ts < ?",
                    (dash.utcms() - dash.TELE_RETENTION_S * 1000,))
        con.commit()
    except Exception:
        pass


_db_total_tokens_cache: tuple = (0, 0.0)


def telemetry_history(window_s: int, target: int = TELE_TARGET_POINTS,
                      session_id: str = "") -> dict:
    """Downsampled telemetry for a window (seconds). Prefers the SQLite store;
    falls back to the in-memory series (first ~40 min) when the DB is still
    empty (fresh start / before it fills). Returns {points, window_s, raw,
    count} where points is the same shape the charts already consume.
    When session_id is given, only samples tagged with that session are used."""
    window_s = int(window_s)
    now_ms = dash.utcms()
    cutoff = now_ms - window_s * 1000
    rows: list[dict] = []
    try:
        con = dash._tele_db()
        try:
            dash._purge_old(con)
            if session_id:
                rows = [dash._row_to_point(r) for r in con.execute(
                    "SELECT ts,cpu_frac,cpu_pct,ram_mb,vram_mb,gpu_pct,"
                    "tps,ptps,tokens,compact,pid,total_tokens,ctx_tokens,"
                    "decoded_tokens FROM telemetry "
                    "WHERE ts>=? AND session_id=? ORDER BY ts",
                    (cutoff, session_id)).fetchall()]
            else:
                rows = [dash._row_to_point(r) for r in con.execute(
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
        with dash._tele_lock:
            rows = sorted(
                ({k: p.get(k) for k in dash._POINT_KEYS} for p in dash._tele_series
                 if p.get("t") and p["t"] >= cutoff),
                key=lambda p: p["t"])
    pts = dash._downsample(rows, target)
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
            win_total = dash._tele_meta.get("total_tokens", 0)
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
