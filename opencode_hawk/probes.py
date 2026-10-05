"""System and llama-server probes: process CPU / memory / IO, llama-server discovery and activity, GPU, disk and RAM readings (Windows PDH / ctypes, Linux via hawk_linux).

Split out of coordinator.py and still part of its namespace: every coordinator-level name is used as
`core.<name>` and coordinator re-binds the names defined here, so a patch on the coordinator module
(tests, the dashboard's sampler singletons) reaches this code. Import it through coordinator, not directly."""
from __future__ import annotations
import json
import os
import re
import subprocess
import threading
import time
import urllib.parse
import urllib.request
import uuid
from . import hawk_linux

from . import coordinator as core  # shared namespace: see the module docstring


# ── llama.cpp activity probe ─────────────────────────────────────────────────
class ProcessTimes:
    """Thin ctypes wrapper to read process CPU + wall time on Windows (stdlib)."""

    def __init__(self):
        self.posix = os.name != "nt"
        if self.posix:
            return  # psutil-backed (hawk_linux.process_times)
        import ctypes
        from ctypes import wintypes
        self.ctypes = ctypes
        self.wintypes = wintypes

        class FILETIME(ctypes.Structure):
            _fields_ = [("dwLowDateTime", wintypes.DWORD),
                        ("dwHighDateTime", wintypes.DWORD)]

        self.FILETIME = FILETIME
        k32 = ctypes.windll.kernel32
        OpenProcess = k32.OpenProcess
        OpenProcess.restype = wintypes.HANDLE
        OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        self.OpenProcess = OpenProcess
        GetProcessTimes = k32.GetProcessTimes
        GetProcessTimes.argtypes = [wintypes.HANDLE,
                                    ctypes.POINTER(FILETIME), ctypes.POINTER(FILETIME),
                                    ctypes.POINTER(FILETIME), ctypes.POINTER(FILETIME)]
        GetProcessTimes.restype = wintypes.BOOL
        self.GetProcessTimes = GetProcessTimes
        GetSystemTimeAsFileTime = k32.GetSystemTimeAsFileTime
        GetSystemTimeAsFileTime.argtypes = [ctypes.POINTER(FILETIME)]
        self.GetSystemTimeAsFileTime = GetSystemTimeAsFileTime

    def _ms(self, ft) -> int:
        return (getattr(ft, "dwHighDateTime", 0) << 32
                | getattr(ft, "dwLowDateTime", 0)) // 10000

    def sample(self, pid: int):
        """Return (wall_ms, cpu_ms) for the given PID, or None if unavailable."""
        if self.posix:
            return hawk_linux.process_times(pid)
        h = self.OpenProcess(0x0400, False, int(pid))
        if not h:
            return None
        try:
            c = self.FILETIME(); e = self.FILETIME()
            k = self.FILETIME(); u = self.FILETIME(); t = self.FILETIME()
            if not self.GetProcessTimes(h, self.ctypes.byref(c), self.ctypes.byref(e),
                                        self.ctypes.byref(k), self.ctypes.byref(u)):
                return None
            self.GetSystemTimeAsFileTime(self.ctypes.byref(t))
            return (self._ms(t), self._ms(k) + self._ms(u))
        finally:
            try:
                self.ctypes.windll.kernel32.CloseHandle(h)
            except Exception:
                pass


_PROC_TIMES = None  # lazy singleton


class ProcessMemIo:
    """ctypes wrapper for process working-set (RAM) and disk IO counters."""

    def __init__(self):
        self.posix = os.name != "nt"
        if self.posix:
            return  # psutil-backed (hawk_linux.process_mem_io)
        import ctypes
        from ctypes import wintypes
        self.ctypes = ctypes

        class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD),
                        ("PageFaultCount", wintypes.DWORD),
                        ("PeakWorkingSetSize", ctypes.c_size_t),
                        ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t),
                        ("PeakPagefileUsage", ctypes.c_size_t)]

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [("ReadOperationCount", ctypes.c_uint64),
                        ("WriteOperationCount", ctypes.c_uint64),
                        ("OtherOperationCount", ctypes.c_uint64),
                        ("ReadTransferCount", ctypes.c_uint64),
                        ("WriteTransferCount", ctypes.c_uint64),
                        ("OtherTransferCount", ctypes.c_uint64)]

        self.PMC = PROCESS_MEMORY_COUNTERS
        self.IOC = IO_COUNTERS
        k32 = ctypes.windll.kernel32
        self.OpenProcess = k32.OpenProcess
        self.OpenProcess.restype = wintypes.HANDLE
        self.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        self.GetProcessMemoryInfo = ctypes.windll.psapi.GetProcessMemoryInfo
        self.GetProcessIoCounters = k32.GetProcessIoCounters
        self.CloseHandle = k32.CloseHandle

    def sample(self, pid: int):
        """Return (working_set_bytes, io_read_bytes, io_write_bytes) or None."""
        if self.posix:
            return hawk_linux.process_mem_io(pid)
        h = self.OpenProcess(0x0400, False, int(pid))  # PROCESS_QUERY_INFORMATION
        if not h:
            return None
        try:
            pmc = self.PMC()
            pmc.cb = self.ctypes.sizeof(self.PMC)
            if not self.GetProcessMemoryInfo(h, self.ctypes.byref(pmc), pmc.cb):
                return None
            ioc = self.IOC()
            if not self.GetProcessIoCounters(h, self.ctypes.byref(ioc)):
                return (int(pmc.WorkingSetSize), 0, 0)
            return (int(pmc.WorkingSetSize),
                    int(ioc.ReadTransferCount), int(ioc.WriteTransferCount))
        finally:
            try:
                self.CloseHandle(h)
            except Exception:
                pass


_PROC_MEMIO = None  # lazy singleton


def llama_mem_io(pid: int | None):
    """Working set + disk IO counters for the llama process (stdlib ctypes)."""
    if not pid:
        return None
    if core._PROC_MEMIO is None:
        core._PROC_MEMIO = core.ProcessMemIo()
    try:
        return core._PROC_MEMIO.sample(pid)
    except Exception as e:
        core.log_debug("llama_mem_io: %s" % e)
        return None


def llama_api_base(cfg) -> str:
    port = int(cfg.get("llama_api_port", 1234))
    host = str(cfg.get("llama_api_host", "127.0.0.1"))
    return "http://%s:%d" % (host, port)


def llama_slots(cfg):
    """Query llama-server /slots for the live KV-cache/context state, or None.
    Sample fields: n_ctx, n_prompt_tokens, n_prompt_tokens_cache,
    n_prompt_tokens_processed, is_processing, id_task."""
    import urllib.request
    try:
        with urllib.request.urlopen(core.llama_api_base(cfg) + "/slots", timeout=2.0) as r:
            d = json.loads(r.read().decode("utf-8", "replace"))
        if isinstance(d, list) and d:
            s = d[0]
            nt = s.get("next_token")
            if isinstance(nt, list):
                nt = nt[0] if nt else {}
            if not isinstance(nt, dict):
                nt = {}
            return {
                "n_ctx": int(s.get("n_ctx") or 0),
                "prompt_tokens": int(s.get("n_prompt_tokens") or 0),
                "cache_tokens": int(s.get("n_prompt_tokens_cache") or 0),
                "processed_tokens": int(s.get("n_prompt_tokens_processed") or 0),
                "decoded_tokens": int(nt.get("n_decoded") or 0),
                "is_processing": bool(s.get("is_processing")),
                "id_task": s.get("id_task"),
            }
    except Exception:
        return None
    return None


def llama_pid(cfg) -> int | None:
    """Return the llama-server process id (config override, else auto-detect by
    image name), or None if not found."""
    pid = int(cfg.get("llama_pid") or 0)
    if pid:
        return pid
    if os.name != "nt":
        return hawk_linux.find_pid("llama-server")
    try:
        out = subprocess.run(["tasklist", "/FI", "IMAGENAME eq llama-server.exe",
                              "/FO", "CSV", "/NH"],
                             capture_output=True, text=True, encoding="utf-8",
                             errors="replace", timeout=15,
                             creationflags=core.creation_flags()).stdout
        for m in re.finditer(r'"([^"]+)","(\d+)"', out):
            if "llama" in m.group(1).lower():
                return int(m.group(2))
    except Exception as e:
        core.log_debug("llama_pid: %s" % e)
    return None


def llama_server_alive(cfg) -> bool:
    """True when the llama-server process exists. Uses llama_pid() which checks
    tasklist; returns False when no llama-server.exe is found. This is the
    liveness watchdog signal: when False, the server has died and the monitor
    must trigger a restart immediately, bypassing all cooldown/cadence logic."""
    return core.llama_pid(cfg) is not None


def llama_respawn(cfg, timeout_s=None) -> bool:
    """Respawn llama-server via its bat file and wait for /health to return 200.
    Called by the watchdog when the server is detected dead. Returns True when
    the server is back and healthy, False on timeout or launch failure.

    Capture discipline: stdout/stderr go to the canonical console log
    (llama-server.out.log, same target llama_restart._respawn uses), never
    DEVNULL, so the dashboard tailer can see the new era. While a server is
    already present this is a no-op that reports True - the liveness watchdog
    and the monitor supervisor both run this path and must never double-
    spawn an extra instance."""
    if core.llama_server_alive(cfg):
        core.log("LLAMA-DOWN: server already alive (pid %s); skipping respawn"
            % core.llama_pid(cfg))
        return True
    bat = (cfg.get("llama_bat_path") or "").strip()
    if not bat:
        core.log("LLAMA-DOWN: llama_bat_path is not set; cannot respawn llama-server")
        return False
    core.log("LLAMA-DOWN: respawning via %s" % bat)
    try:
        from . import llama_restart as _lr
        log_fn = _lr.LOG_FN
    except Exception:
        log_fn = "llama-server.out.log"
    log_path = core.home_dir() / log_fn
    try:
        fh = open(log_path, "ab")
    except Exception:
        fh = None
    try:
        if os.name == "nt":
            argv = ["cmd", "/c", bat]
        else:
            argv = [bat] if os.access(bat, os.X_OK) else ["sh", bat]
        proc = subprocess.Popen(
            argv,
            stdout=fh, stderr=subprocess.STDOUT,
            creationflags=core.creation_flags(),
        )
    except Exception as e:
        core.log("LLAMA-DOWN: respawn launch failed: %s" % e)
        return False
    finally:
        if fh:
            try:
                fh.close()
            except Exception:
                pass
    # Wait for health (model load takes time; use the same timeout as restart).
    if timeout_s is None:
        timeout_s = int(cfg.get("llama_restart_health_timeout_s", 240))
    base = core.llama_api_base(cfg)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(base + "/health", timeout=3) as r:
                if r.status == 200:
                    core.log("LLAMA-DOWN: server back up and healthy (pid %s, "
                        "%ds)" % (core.llama_pid(cfg), int(time.time() - (deadline - timeout_s))))
                    return True
        except Exception:
            pass
        time.sleep(3.0)
    core.log("LLAMA-DOWN: respawn timed out after %ds; server may need manual attention"
        % timeout_s)
    return False


def llama_cpu_frac(cfg, state, agent_window_s: int | None = None) -> float:
    """Return an estimate of llama-server CPU activity over the recent window:
    cpu_fraction = llama CPU-seconds / wall-seconds in the window. 0.0 means
    idle/unavailable. Persists the last sample in state for cross-poll deltas.

    We sample now and compare against the previous sample + its wall timestamp;
    the effective window is (now - previous wall time), clamped to
    stall_llama_window_s so we look back at most that far."""
    pid = core.llama_pid(cfg)
    if not pid:
        return 0.0
    if core._PROC_TIMES is None:
        core._PROC_TIMES = core.ProcessTimes()
    now = int(time.time() * 1000)
    samp = core._PROC_TIMES.sample(pid)
    if not samp:
        return 0.0
    wall_now, cpu_now = samp
    prev = state.get("_llama")
    maxback = int(cfg.get("stall_llama_window_s", 120)) * 1000
    if isinstance(prev, dict) and prev.get("cpu") is not None:
        d_cpu = max(0, cpu_now - int(prev["cpu"]))
        d_wall = now - int(prev["wall"])
        if 0 < d_wall <= maxback * 1.5:
            frac = d_cpu / d_wall
        else:
            # Too long since last sample: cannot attribute old CPU to now. Use a
            # short self-sample over agent_window_s if provided, else treat the
            # fresh sample as the baseline (frac 0 until the next sample).
            if agent_window_s:
                time.sleep(min(agent_window_s, 5))
                samp2 = core._PROC_TIMES.sample(pid)
                if samp2:
                    wall_now, cpu_now = samp2
                    d_cpu = max(0, cpu_now - samp[1])
                    d_wall = wall_now - samp[0]
                    frac = d_cpu / d_wall if d_wall > 0 else 0.0
                else:
                    frac = 0.0
            else:
                frac = 0.0
    else:
        frac = 0.0
    state["_llama"] = {"wall": now, "cpu": cpu_now}
    return max(0.0, min(frac, 8.0))


def llama_slots_processing(cfg) -> bool:
    """True if llama-server reports slot 0 actively processing (prompt/decode).
    False when the server is unreachable (treated as 'no signal', not 'active')."""
    try:
        slots = core.llama_slots(cfg)
        return bool(slots and slots.get("is_processing"))
    except Exception as e:
        core.log_debug("llama_slots_processing: %s" % e)
        return False


def llama_token_speeds(cfg, gap_s=0.5):
    """Sample llama-server /slots twice a short gap apart and return
    (output_tps, ingest_tps) — the rate at which tokens are currently being
    decoded vs prompt-processed. (0.0, 0.0) when unreachable. A positive
    output speed means the model is mid-generation even if the server didn't
    flag is_processing on this build."""
    s1 = core.llama_slots(cfg)
    if not s1:
        return 0.0, 0.0
    t0 = time.monotonic()
    time.sleep(gap_s)
    s2 = core.llama_slots(cfg)
    if not s2:
        return 0.0, 0.0
    dt = time.monotonic() - t0
    if dt <= 0 or not s1.get("id_task") or not s2.get("id_task") \
            or s1.get("id_task") != s2.get("id_task"):
        # Task boundary (new task started between samples) or no active task:
        # treat as untrusted — the caller decides how to treat this.
        return 0.0, 0.0
    out = max(0.0, (s2.get("decoded_tokens", 0) - s1.get("decoded_tokens", 0))) / dt
    ingest = max(0.0, (s2.get("processed_tokens", 0) - s1.get("processed_tokens", 0))) / dt
    return out, ingest


# GPU engine utilisation, the same universal, no-3rd-party method Task Manager
# uses: PDH '\GPU Engine(*)\Utilization Percentage'. We read the LARGEST single
# engine value across ALL processes — the per-pid filter read 0 on this AMD
# driver mid-generation, but the all-engine max reports the real load (~90-95%
# while llama generates, <10% idle). This is the single source of truth for the
# GPU read: both the dashboard telemetry loop and llama_gpu_util() call
# gpu_engine_max() below.
#
# Ported 1:1 from the working telemetry_monitor_fast.ahk: one persistent PDH
# query (opened once) whose PdhCollectQueryData is called once per read. Two
# PDH quirks the AHK relies on — and we must keep:
#   * the size query (pItems=NULL) returns a NON-ZERO status yet still fills the
#     buffer-size/count out-params, so we must NOT bail on that return code;
#   * the fill's 6th param (pdwLength) is passed 0 by value (PDH sizes from #3).
_GPU_SYS_COUNTER = r"\GPU Engine(*)\Utilization Percentage"
_GPU_MEM_COUNTER = r"\GPU Adapter Memory(*)\Dedicated Usage"
_DISK_IDLE_COUNTER = r"\PhysicalDisk(*)\% Idle Time"
_PDH_GPU = {"ok": False, "err": None, "q": None, "c": None, "mc": None, "dc": None}
_PDH_GPU_LOCK = threading.Lock()


def _pdh_gpu_init() -> bool:
    """Open the persistent all-engine GPU counter query once (returns ok)."""
    if core._PDH_GPU["ok"] or core._PDH_GPU["err"] is not None:
        return core._PDH_GPU["ok"]
    if os.name != "nt":
        core._PDH_GPU["err"] = "not windows"
        return False
    try:
        import ctypes as C
        pdh = C.windll.pdh
        pdh.PdhOpenQueryW.argtypes = [C.c_void_p, C.c_void_p,
                                      C.POINTER(C.c_void_p)]
        pdh.PdhOpenQueryW.restype = C.c_long
        pdh.PdhAddEnglishCounterW.argtypes = [
            C.c_void_p, C.c_wchar_p, C.c_ulong, C.POINTER(C.c_void_p)]
        pdh.PdhAddEnglishCounterW.restype = C.c_long
        pdh.PdhCollectQueryData.argtypes = [C.c_void_p]
        pdh.PdhCollectQueryData.restype = C.c_long
        pdh.PdhGetFormattedCounterArrayW.argtypes = [
            C.c_void_p, C.c_ulong, C.POINTER(C.c_ulong),
            C.POINTER(C.c_ulong), C.c_void_p, C.c_ulong]
        pdh.PdhGetFormattedCounterArrayW.restype = C.c_long
        q = C.c_void_p()
        if pdh.PdhOpenQueryW(None, None, C.byref(q)) != 0:
            core._PDH_GPU["err"] = "PdhOpenQueryW"
            return False
        c = C.c_void_p()
        if pdh.PdhAddEnglishCounterW(q, core._GPU_SYS_COUNTER, 0,
                                     C.byref(c)) != 0:
            pdh.PdhCloseQuery(q)
            core._PDH_GPU["err"] = "PdhAddEnglishCounterW"
            return False
        mc = C.c_void_p()
        if pdh.PdhAddEnglishCounterW(q, core._GPU_MEM_COUNTER, 0,
                                     C.byref(mc)) != 0:
            mc = None  # adapter-memory counter absent -> engine-only mode
        dc = C.c_void_p()
        if pdh.PdhAddEnglishCounterW(q, core._DISK_IDLE_COUNTER, 0,
                                     C.byref(dc)) != 0:
            dc = None  # disk counter absent (rare) -> no disk activity feed
        core._PDH_GPU.update(q=q, c=c, mc=mc, dc=dc, ok=True)
        pdh.PdhCollectQueryData(q)  # prime; first sample is 0
        return True
    except Exception as e:
        core._PDH_GPU["err"] = str(e)
        core.log_debug("pdh gpu init: %s" % e)
        return False


def _arr_vals(h) -> list:
    """Per-instance (name, value) list for a (*)-style PDH counter. Returns []
    when the counter is unavailable or the array cannot be read. Items are
    PDH_FMT_COUNTERVALUE_ITEM: PWSTR szName (offset 0) + PDH_FMT_COUNTERVALUE
    (CStatus DWORD at offset 8, value double at offset 16). Rows whose CStatus
    is invalid (first sample or a transient fail) are dropped, never treated
    as real data."""
    if not h:
        return []
    import ctypes as C
    pdh = C.windll.pdh
    sz = C.c_ulong()
    cnt = C.c_ulong()
    pdh.PdhGetFormattedCounterArrayW(h, 0x200, C.byref(sz), C.byref(cnt),
                                     None, 0)
    if cnt.value == 0 or sz.value < 24:
        return []
    buf = (C.c_ubyte * sz.value)()
    if pdh.PdhGetFormattedCounterArrayW(h, 0x200, C.byref(sz), C.byref(cnt),
                                        buf, 0) != 0:
        return []
    base = C.addressof(buf)
    out = []
    for i in range(cnt.value):
        item = base + i * 24
        try:
            status = C.c_ulong.from_address(item + 8).value
            if status > 1:  # PDH_CSTATUS_VALID_DATA(0)/NEW_DATA(1) only
                continue
            name_ptr = C.c_longlong.from_address(item).value
            name = C.wstring_at(name_ptr) if name_ptr else ""
        except Exception:
            continue
        v = C.c_double.from_address(item + 16).value
        out.append((name, v))
    return out


def _arr_max(h):
    """Highest (name, value) value for a (*)-style counter, or None."""
    vals = [v for _, v in core._arr_vals(h)]
    return max(vals) if vals else None


def _gpu_group(rows, agg_highest):
    """Group per-engine/-segment PDH rows by adapter LUID.

    WDDM instance names carry the adapter identity as a `luid_<H>_<L>_phys_<N>`
    token run: engine rows are per-PROCESS (`pid_1234_luid_0x…_0x…_phys_0_
    eng_0_engtype_3D`) while adapter-memory rows start with it directly
    (`luid_0x…_0x…_phys_0`). The LUID run is located by finding the `luid`
    token and taking it plus the next four. Rows without a LUID are dropped
    (e.g. a hypothetical _Total). Returns [(luid, value)] in first-seen (WDDM
    enumeration) order: when agg_highest is true the adapter value is the max
    of its engines (a GPU's processing % is its busiest engine — and with
    per-process rows, across all processes sharing the adapter), otherwise the
    sum (total VRAM usage across phys segments)."""
    groups: dict = {}
    order: list = []
    for name, v in rows:
        parts = name.split("_")
        try:
            i = parts.index("luid")
        except ValueError:
            continue
        if i + 5 > len(parts):
            continue
        key = "_".join(parts[i:i + 5])
        if key not in groups:
            groups[key] = 0.0
            order.append(key)
        groups[key] = max(groups[key], v) if agg_highest else groups[key] + v
    return [(k, groups[k]) for k in order]


def _pdh_gpu_read() -> tuple:
    """One fresh collect on the persistent query.

    Returns (max_engine_pct, vram_mb, gpu_mem, gpu_eng, disk_inst): the
    highest single GPU-engine utilisation (0-100, all adapters), the system
    total dedicated GPU memory in use (MB, sum across adapters), per-adapter
    (LUID) dedicated usage [(luid, MB)] and per-adapter engine utilisation
    [(luid, pct)] both in WDDM enumeration order, and the per-physical-disk
    idle % [(instance, idle_pct)]. Empty lists when a counter is absent."""
    if not core._pdh_gpu_init():
        return 0.0, None, [], [], []
    import ctypes as C
    pdh = C.windll.pdh
    q, c, mc = core._PDH_GPU["q"], core._PDH_GPU["c"], core._PDH_GPU["mc"]
    pdh.PdhCollectQueryData(q)

    engine = core._arr_max(c) or 0.0
    gpu_eng = [(k, round(v, 1)) for k, v in core._gpu_group(core._arr_vals(c), True)]
    vram_mb = None
    gpu_mem = []
    if mc:
        rows = core._arr_vals(mc)
        b = core._arr_max(mc)
        if b is not None:
            # GPU Adapter Memory "Dedicated Usage" is reported in bytes
            vram_mb = round(b / (1024.0 * 1024.0), 1)
        gpu_mem = [(k, round(v / (1024.0 * 1024.0), 1))
                   for k, v in core._gpu_group(rows, False)]
    disk_inst = []
    if core._PDH_GPU["dc"]:
        disk_inst = core._arr_vals(core._PDH_GPU["dc"])  # % Idle Time per physical disk
    return engine, vram_mb, gpu_mem, gpu_eng, disk_inst


def gpu_engine_max(state=None) -> float:
    """Highest single GPU-engine utilisation (%) across all processes, or 0.

    Read straight from PDH (persistent query, no PowerShell). PDH collects are
    performed by one background sampler thread so a stalled PdhCollectQueryData
    (observed under concurrent llama model loads) can never freeze the monitor
    poll; callers always get the latest completed sample (~1s fresh)."""
    if os.name != "nt":
        return hawk_linux.gpu_engine_max()
    core._ensure_gpu_sampler()
    return core._PDH_SAMPLE.get("val", 0.0)


_PDH_SAMPLE = {"val": 0.0, "vram_mb": None, "gpu_inst": [], "gpu_eng": [],
               "disk_inst": [], "t": 0.0}


def gpu_adapters_usage_mb() -> list:
    """Per-adapter dedicated GPU VRAM in use (MB): [(luid, mb)] in WDDM
    enumeration order — the same order the dashboard matches to its
    registry-derived adapter totals. [] when the counter is unavailable."""
    if os.name != "nt":
        return [(g["id"], g["used_mb"]) for g in hawk_linux.gpus()]
    core._ensure_gpu_sampler()
    return list(core._PDH_SAMPLE.get("gpu_inst") or [])


def gpu_adapters_engine_pct() -> list:
    """Per-adapter GPU PROCESSING utilisation %: [(luid, pct)] where pct is
    the adapter's busiest engine (3D/Copy/VideoEncode/VideoDecode) — the same
    semantics as the all-engine gauge but scoped per adapter. Same LUID order
    as gpu_adapters_usage_mb(). [] when the counter is unavailable."""
    if os.name != "nt":
        return [(g["id"], g["pct"]) for g in hawk_linux.gpus()]
    core._ensure_gpu_sampler()
    return list(core._PDH_SAMPLE.get("gpu_eng") or [])


def physical_disk_active_pct() -> list:
    """Real-time disk activity % per physical disk: [(instance, pct)] where
    pct = 100 - %Idle Time (Task-Manager "Active time" semantics). Instances
    are plain disk indexes (e.g. "0", "1"); the _Total row is dropped. [] when
    the disk counter is absent (rare failure). On Linux the instances are
    disk names (e.g. "nvme0n1") instead of indexes."""
    if os.name != "nt":
        return hawk_linux.disk_active_pct()
    core._ensure_gpu_sampler()
    out = []
    for name, idle in core._PDH_SAMPLE.get("disk_inst") or []:
        if not name or name in ("_Total", "_"):
            continue
        try:
            int(name.split()[0])  # disk instances are numeric indexes
        except ValueError:
            continue
        out.append((name.split()[0], round(100.0 - float(idle), 1)))
    return out
_GPU_SAMPLER_LOCK = threading.Lock()
_GPU_SAMPLER_STARTED = False


def _ensure_gpu_sampler():
    """Lazily start the one PDH sampling thread that owns the GPU query."""
    if core._GPU_SAMPLER_STARTED:
        return
    if os.name != "nt":
        return
    with core._GPU_SAMPLER_LOCK:
        if core._GPU_SAMPLER_STARTED:
            return
        core._GPU_SAMPLER_STARTED = True
        threading.Thread(target=core._gpu_sampler_loop, daemon=True,
                         name="gpu-sampler").start()


def _gpu_sampler_loop():
    while True:
        try:
            with core._PDH_GPU_LOCK:
                v, vram, gpu_mem, gpu_eng, disk_inst = core._pdh_gpu_read()
            core._PDH_SAMPLE["val"] = round(float(v), 1)
            core._PDH_SAMPLE["vram_mb"] = vram
            core._PDH_SAMPLE["gpu_inst"] = gpu_mem
            core._PDH_SAMPLE["gpu_eng"] = gpu_eng
            core._PDH_SAMPLE["disk_inst"] = disk_inst
            core._PDH_SAMPLE["t"] = time.monotonic()
        except Exception:
            pass  # keep sampling; a transient PDH stall must not end the thread
        time.sleep(1.0)


def gpu_vram_used_mb() -> float | None:
    """System-wide dedicated GPU VRAM in use (MB, max across adapters), or None.

    Backed by the same background PDH sampler as gpu_engine_max(): reads the
    `GPU Adapter Memory(*)\\Dedicated Usage` counter (~1s fresh). None when the
    counter is unavailable (engine-only PDH mode)."""
    if os.name != "nt":
        return hawk_linux.gpu_vram_used_mb()
    core._ensure_gpu_sampler()
    return core._PDH_SAMPLE.get("vram_mb")


def llama_gpu_util(cfg, state=None):
    r"""Return llama-server's GPU utilisation as {'sum': x, 'max': x} (percents),
    or {'sum':0,'max':0} when llama isn't running.

    Reads the SAME all-engine PDH max as the dashboard's gpu_system_max().
    Per-process GPU is NOT available on this box (AMD RX 9060 XT WDDM): the
    "GPU Process" object doesn't exist — verified 4 ways (every
    `\GPU Process(*)\…` PDH path is INVALID_PATH, `typeperf -q "GPU Process"`
    → not found, no Win32_PerfRawData_GPU* WMI classes, no Perflib registry
    object). The all-engine max is therefore NOT a faithful proxy for llama's
    own activity: desktop/compositor/other-app noise sits right at the old
    ~10% idle threshold (observed 10.7% with llama verifiably idle, 2026-09-22,
    which false-blocked self-checks for ~20 min). llama_idle() uses it as a
    fallback signal only — consulted when both direct gates (slots, token
    speeds) are disabled. We still gate on llama being present so an
    unrelated GPU consumer isn't counted as llama activity. Cached in state for
    llama_gpu_cache_s so llama_idle's gate doesn't re-collect every poll."""
    pid = core.llama_pid(cfg)
    if not pid:
        return {"sum": 0.0, "max": 0.0}
    now = time.monotonic()
    if state is not None:
        c = state.get("_gpu")
        if isinstance(c, dict) and c.get("pid") == pid and \
                (now - c.get("t", 0.0)) < float(cfg.get("llama_gpu_cache_s", 5.0)):
            return {"sum": c["sum"], "max": c["max"]}
    x = core.gpu_engine_max()
    res = {"sum": float(x), "max": float(x)}
    if state is not None:
        state["_gpu"] = {"t": now, "pid": pid,
                         "sum": res["sum"], "max": res["max"]}
    return res


# ── Memory totals (VRAM + system RAM) for the dashboard Y-axis pins ─────────
# These back the two "usage" charts' pinned Y-axis and the "used / total (%)"
# tile readout. WMI AdapterRAM is unreliable on this box (reports ~4 GiB for a
# 16 GB card), so the VRAM total comes from the gpu_vram_total_mb config
# (default 16384 == RX 9060 XT) with an opt-in, sanity-bounded DXGI auto-detect
# for when the user sets it to 0.
_GVRAM = {"mb": None}
_SRAM = {"mb": None}


def _dxgi_dedicated_vram_mb() -> float | None:
    """Best-effort dedicated GPU VRAM (MB) via DXGI adapter GetDesc.
    Returns None on ANY failure so the caller falls back to the config default.
    Opt-in only (used when gpu_vram_total_mb == 0); not on the default path."""
    try:
        import ctypes as C
        from ctypes import wintypes
        import uuid

        C.windll.dxgi.LoadLibrary()
        C.windll.dxgi.CreateDXGIFactory1.argtypes = [C.c_void_p, C.c_void_p]
        C.windll.dxgi.CreateDXGIFactory1.restype = C.c_long

        def refiid(hexstr):
            return (C.c_ubyte * 16).from_buffer_copy(
                uuid.UUID(hexstr).bytes_le)

        # IDXGIFactory1: {77da53c0-9989-407a-b0b6-c6999a8acaf3}
        iidf = refiid("77da53c0-9989-407a-b0b6-c6999a8acaf3")
        factory = C.c_void_p()
        if C.windll.dxgi.CreateDXGIFactory1(C.byref(iidf),
                                           C.byref(factory)) != 0 or not factory:
            return None
        def vtable(ptr):
            return C.cast(C.cast(ptr, C.c_void_p),
                          C.POINTER(C.c_void_p * 16))[0]

        # IDXGIFactory vtable: index 3 = GetAdapter(UINT, IDXGIAdapter**)
        get_adapter = C.cast(
            vtable(factory)[3],
            C.CFUNCTYPE(C.c_long, C.c_void_p, C.c_uint, C.POINTER(C.c_void_p)))
        adapter = C.c_void_p()
        if get_adapter(factory, 0, C.byref(adapter)) != 0 or not adapter:
            return None
        # IDXGIAdapter vtable: index 3 = GetDesc(OUT IDXGI_ADAPTER_DESC*)
        get_desc = C.cast(
            vtable(adapter)[3], C.CFUNCTYPE(C.c_long, C.c_void_p, C.c_void_p))
        # IDXGI_ADAPTER_DESC (x64): DedicatedVideoMemory (SIZE_T) is the second
        # SIZE_T after Description[128]/DeviceId/DeviceLuid/Flags/three UINTs/
        # DriverVersion. Read a wide window and sanity-check the value instead of
        # trusting an exact offset (opt-in path; wrong read -> None -> fallback).
        desc = (C.c_ubyte * 384)()
        if get_desc(adapter, desc) != 0:
            return None
        for off in range(280, 328, 8):
            dv = int.from_bytes(desc[off:off + 8], "little")
            mb = dv / (1024 * 1024)
            if 1024 <= mb <= 256 * 1024:  # 1 GiB .. 256 GiB is plausible
                return mb
        return None
    except Exception:
        return None


def gpu_adapter_totals() -> list[dict]:
    """[{name, total_mb}] — one entry per adapter in the video-class registry
    (WDDM enumeration order): DriverDesc from each subkey, total_mb from the
    QWORD 'HardwareInformation.qwMemorySize' (the real dedicated VRAM; WMI
    AdapterRAM is 32-bit capped), 0 for UMA/virtual adapters without one. This
    mirrors the per-adapter PDH engine rows one-for-one, so the dashboard can
    pair names/totals positionally. On Linux: one entry per GPU from
    hawk_linux.gpus(), in the same order as gpu_adapters_usage_mb(). Empty on
    failure."""
    out: list[dict] = []
    if os.name != "nt":
        return [{"name": g["name"], "total_mb": g["total_mb"]}
                for g in hawk_linux.gpus()]
    try:
        cmd = ('powershell.exe -NoProfile -NonInteractive -Command '
               '"Get-ChildItem \'HKLM:\\SYSTEM\\CurrentControlSet\\Control\\'
               'Class\\{4d36e968-e325-11ce-bfc1-08002be10318}\' | '
               'Where-Object { $_.PSChildName -match \'^\\d{4}$\' } | '
               'ForEach-Object { $p = Get-ItemProperty $_.PSPath; '
               '$v = $p.\'HardwareInformation.qwMemorySize\'; '
               '$s = 0; if ($null -ne $v) { $s = $v }; '
               '\'{0}|{1}\' -f $p.DriverDesc, $s }"')
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                             creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                             timeout=10).stdout
        for ln in res.splitlines():
            ln = ln.strip()
            if "|" not in ln:
                continue
            name, _, size = ln.rpartition("|")
            try:
                mb = int(size) / (1024.0 * 1024.0)
            except ValueError:
                continue
            out.append({"name": name.strip(), "total_mb": mb})
    except Exception:
        return []
    return out


def gpu_vram_total_mb() -> float:
    """Total GPU VRAM (MB) for pinning the VRAM chart. The config override
    (default 16384) wins when > 0; when set to 0, try DXGI auto-detect; else the
    default. Cached — the total does not change at runtime."""
    if core._GVRAM["mb"] is None:
        val = float(core.load_config().get("gpu_vram_total_mb", 0) or 0)
        if val > 0:
            core._GVRAM["mb"] = val
        else:
            if os.name == "nt":
                auto = core._dxgi_dedicated_vram_mb() or max(
                    (g["total_mb"] for g in core.gpu_adapter_totals()), default=0) or None
            else:
                auto = hawk_linux.gpu_vram_total_mb()
            # 16 GB only pins the chart axis when no GPU could be detected
            core._GVRAM["mb"] = float(auto) if auto else 16384.0
    return float(core._GVRAM["mb"])


def system_ram_total_mb() -> float:
    """Total physical RAM (MB) via GlobalMemoryStatusEx. 0 when unavailable."""
    if core._SRAM["mb"] is None and os.name != "nt":
        core._SRAM["mb"] = hawk_linux.ram_total_mb()
    if core._SRAM["mb"] is None:
        mb = 0.0
        try:
            import ctypes as C
            from ctypes import wintypes

            # NOTE: this x64 build's GlobalMemoryStatusEx requires dwLength=64
            # (the natural ctypes layout of the 6 SIZE_T fields is 56 bytes), so a
            # trailing 8-byte pad is added to match and avoid the API writing past
            # the buffer. Field offsets (dwTotalPhys @ 8) were verified live.
            class MEMORYSTATUSEx(C.Structure):
                _fields_ = [("dwLength", wintypes.DWORD),
                            ("dwMemoryLoad", wintypes.DWORD),
                            ("dwTotalPhys", C.c_size_t),
                            ("dwAvailPhys", C.c_size_t),
                            ("dwTotalPageFile", C.c_size_t),
                            ("dwAvailPageFile", C.c_size_t),
                            ("dwTotalVirtual", C.c_size_t),
                            ("dwAvailVirtual", C.c_size_t),
                            ("_pad", C.c_uint64)]

            ms = MEMORYSTATUSEx()
            ms.dwLength = C.sizeof(MEMORYSTATUSEx)
            if C.windll.kernel32.GlobalMemoryStatusEx(C.byref(ms)):
                mb = int(ms.dwTotalPhys) / (1024 * 1024)
        except Exception:
            mb = 0.0
        core._SRAM["mb"] = mb
    return float(core._SRAM["mb"])


def system_ram_used_mb() -> float | None:
    """System-wide physical RAM in use (MB) via GlobalMemoryStatusEx.

    Matches Task Manager's "in use" (= total - available; available is the
    sum of the standby, free and zero lists). Fresh query every call: RAM use
    drifts, so unlike system_ram_total_mb() this is not cached. None when the
    API call fails."""
    try:
        import ctypes as C
        from ctypes import wintypes

        # same padded layout as system_ram_total_mb() (see note there)
        class MEMORYSTATUSEx(C.Structure):
            _fields_ = [("dwLength", wintypes.DWORD),
                        ("dwMemoryLoad", wintypes.DWORD),
                        ("dwTotalPhys", C.c_size_t),
                        ("dwAvailPhys", C.c_size_t),
                        ("dwTotalPageFile", C.c_size_t),
                        ("dwAvailPageFile", C.c_size_t),
                        ("dwTotalVirtual", C.c_size_t),
                        ("dwAvailVirtual", C.c_size_t),
                        ("_pad", C.c_uint64)]

        if os.name != "nt":
            return hawk_linux.ram_used_mb()
        ms = MEMORYSTATUSEx()
        ms.dwLength = C.sizeof(MEMORYSTATUSEx)
        if not C.windll.kernel32.GlobalMemoryStatusEx(C.byref(ms)):
            return None
        used = int(ms.dwTotalPhys) - int(ms.dwAvailPhys)
        if used < 0:
            return None
        return round(used / (1024 * 1024), 1)
    except Exception:
        return None


def llama_idle(cfg, state, agent_window_s: int | None = None) -> bool:
    """True when the llama model is NOT meaningfully active (idle or absent).

    "Active" = CPU fraction at/above stall_llama_cpu_frac (10% default), OR a
    llama-server slot is currently processing, OR tokens are currently being
    decoded or prompt-processed (output tps or ingest tps > 0). The slots and
    token-speed checks cover the heavy-GPU-offload case where the CPU looks
    idle while text is still being generated.

    The GPU gate (stall_gate_gpu) is a FALLBACK ONLY: gpu_engine_max() is
    machine-wide (all processes — this WDDM box exposes no per-process GPU
    counter), so desktop/compositor/other-app noise sits right at the 10%
    threshold and false-trips it for minutes at a time (observed 2026-09-22:
    blocked every poll 17:42-18:01 while llama was verifiably idle). When the
    direct signals (slots/tokens) are enabled they fully cover generation, so
    the GPU reading is consulted only when BOTH are disabled (server builds
    that expose neither)."""
    frac = core.llama_cpu_frac(cfg, state, agent_window_s)
    thresh = float(cfg.get("stall_llama_cpu_frac", 0.10))
    if frac >= thresh:
        return False
    # CPU looks idle, but under heavy GPU offload the CPU can be quiet while
    # text is still being generated. Ask llama-server directly.
    slots_on = bool(cfg.get("stall_gate_slots", True))
    if slots_on and core.llama_slots_processing(cfg):
        return False
    # Token speeds: a generation that is mid-decode moves decoded_tokens even
    # when  is_processing is not reported (varies by server version). Both
    # output AND ingest speeds must be zero for the model to count as idle.
    tokens_on = bool(cfg.get("stall_gate_tokens", True))
    if tokens_on:
        out_tps, ingest_tps = core.llama_token_speeds(cfg)
        if out_tps > 0.0 or ingest_tps > 0.0:
            return False
    # Machine-wide GPU reading — consulted ONLY when the direct signals above
    # are unavailable (see docstring). Never as an independent veto.
    if cfg.get("stall_gate_gpu", True) and not (slots_on or tokens_on):
        gpu = core.llama_gpu_util(cfg, state)
        if gpu and float(gpu.get("max", 0.0)) >= \
                float(cfg.get("stall_llama_gpu_util", 10.0)):
            return False
    return True


def llama_idle_sustained(cfg, state) -> bool:
    """llama_idle confirmed by a second sample stall_idle_confirm_gap_s later.

    The local llama-server is SHARED with the coordinator's own opencode
    instance (this session and small_model chatter — compaction, titles — all
    hit the same server/slots as the worker), so a single instantaneous busy
    sample is usually a short blip of OUR OWN harness, not the worker
    generating. The injection gates therefore require the server to be busy on
    BOTH samples: a real worker turn (minutes long) survives both, a
    compaction/title blip (seconds) usually does not. gap <= 0 = old single
    sample behavior."""
    if core.llama_idle(cfg, state):
        return True
    gap = int(cfg.get("stall_idle_confirm_gap_s", 30))
    if gap <= 0:
        return False
    time.sleep(min(gap, 55))
    return core.llama_idle(cfg, state)


def build_continue_cmd(cli, attach, session_id, project_dir, message, auto=True,
                       creds=None, agent=""):
    # Both paths pass --auto unless auto=False: "auto-approve permissions that
    # are not explicitly denied", so Hawk-driven continues never block on an
    # access prompt. auto=False (an answer/critical injection) omits it.
    if attach:
        # Attach to the Desktop's own sidecar so the turn is generated there and
        # the GUI streams it live (requirement: user sees worker progress).
        cmd = [cli, "run", "--attach", attach, "--session", session_id]
    else:
        cmd = [cli, "run", "--session", session_id]
    if agent:
        # Keep the session's agent: without --agent the turn runs as OpenCode's default agent.
        cmd += ["--agent", agent]
    if auto:
        cmd.append("--auto")
    if attach and creds:
        # The Desktop sidecar enforces HTTP Basic auth with per-launch creds
        # (desktop_sidecar()); without them the attach dies with 401 and the
        # injected message never lands. The CLI defaults the username to
        # "opencode", but pass both explicitly for robustness.
        cmd += ["--username", creds.get("username") or "opencode",
                "--password", creds.get("password") or ""]
    cmd += ["--dir", project_dir, message]
    return cmd


def send_continue(cfg, session_id, project_dir, message, auto=True) -> bool:
    cli = core.find_opencode()
    if not cli:
        return False
    sidecar = None
    if cfg.get("continue_via_attach", True):
        # desktop_sidecar() resolves the listener AND its per-launch Basic auth
        # creds in one call (find_desktop_server() alone would attach without
        # creds and be 401-rejected - see build_continue_cmd).
        sidecar = core.desktop_sidecar()
    attach = sidecar[0] if sidecar else None
    creds = sidecar[1] if sidecar else None
    cmd = core.build_continue_cmd(cli, attach, session_id, project_dir, message,
                             auto=auto, creds=creds, agent=core.session_agent(session_id))
    core.log("injecting continue via %s" %
        ("attach %s" % attach if attach else "standalone server (desktop sidecar not found)"))
    core.log("cmd: %s" % " ".join(cmd[:6]))
    try:
        flags = core.creation_flags()
        # Fire-and-forget: the CLI = passive client in attach mode; the Desktop
        # sidecar owns the turn. We never block on the full turn (local models
        # can take many minutes); the next polls see the session busy.
        subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            creationflags=flags,
        )
        return True
    except Exception as e:
        core.log("continue injection error: %s" % e)
        return False


def _session_project_dir(session_id: str) -> str:
    """Resolve the active leaf session's project directory for continue
    injection (mirrors the lookup poll_multisession performs)."""
    try:
        con = core.connect_db(core.db_path())
        try:
            base = core.session_row(con, session_id)
            if base is None:
                return ""
            leaf = core.active_leaf(con, base)
            return (leaf.get("directory") or "").strip()
        finally:
            con.close()
    except Exception:
        return ""


def _reply_to_session(cfg, session_id: str, choice: str) -> bool:
    """Deliver a mobile (ntfy) reply back into the session as a continue.
    The raw text is injected 1:1 without wrapping."""
    try:
        project_dir = core._session_project_dir(session_id) or cfg.get("project_dir") or ""
        if not project_dir:
            core.log("notify: cannot reply to %s: no project dir" % session_id)
            return False
        ok = core.send_continue(cfg, session_id, project_dir, choice, auto=False)
        core.log("notify: ntfy reply %r -> %s %s"
            % (choice[:60], session_id, "OK" if ok else "FAILED"))
        return ok
    except Exception as e:
        core.log("notify: reply-to-session failed: %s" % e)
        return False
