"""Linux implementations of Hawk's system probes.

coordinator.py and dashboard.py read GPU, disk, RAM and process data through
Windows APIs (PDH, WMI, ctypes). On Linux they delegate here instead:

- GPU: AMD cards via the amdgpu sysfs files (/sys/class/drm/card*/device),
  NVIDIA cards via `nvidia-smi`. No extra tools are needed for AMD.
- Per-process VRAM: the DRM fdinfo of the process (amdgpu), else nvidia-smi.
- Disk activity: psutil per-disk busy time (whole disks only).
- RAM, process CPU / memory / IO: psutil.

Every probe returns an empty result (or None) instead of raising when the
data is unavailable, matching the Windows probes' contract.
"""
from __future__ import annotations

import glob
import os
import re
import shutil
import subprocess
import threading
import time

try:
    import psutil
except ImportError:  # pragma: no cover - psutil is in requirements.txt
    psutil = None

MIB = 1024 * 1024
DRM_ROOT = "/sys/class/drm"
SYS_BLOCK = "/sys/block"
PROC_ROOT = "/proc"
_GPU_TTL_S = 1.0
_lock = threading.Lock()
_gpu_cache = {"at": 0.0, "gpus": []}
_name_cache: dict[str, str] = {}
_disk_prev: dict = {"t": 0.0, "busy": {}}


def _read(path: str) -> str | None:
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read().strip()
    except OSError:
        return None


def _read_int(path: str) -> int | None:
    v = _read(path)
    try:
        return int(v) if v is not None else None
    except ValueError:
        return None


# ── GPUs ─────────────────────────────────────────────────────────────────────
def _card_sort_key(path: str) -> int:
    m = re.search(r"card(\d+)$", path)
    return int(m.group(1)) if m else 0


def _amd_name(dev: str, card: str) -> str:
    """Marketing name of an amdgpu card (cached): product_name when the
    driver exposes it, else `lspci`, else a generic label."""
    if card in _name_cache:
        return _name_cache[card]
    name = _read(os.path.join(dev, "product_name")) or ""
    if not name and shutil.which("lspci"):
        slot = ""
        for ln in (_read(os.path.join(dev, "uevent")) or "").splitlines():
            if ln.startswith("PCI_SLOT_NAME="):
                slot = ln.split("=", 1)[1]
        if slot:
            try:
                out = subprocess.run(["lspci", "-mm", "-s", slot],
                                     capture_output=True, text=True,
                                     timeout=5).stdout
                # "03:00.0" "VGA compatible controller" "AMD/ATI" "Navi 44 [Radeon RX 9060 XT]" ...
                fields = re.findall(r'"([^"]*)"', out)
                if len(fields) >= 3:
                    m = re.search(r"\[([^\]]+)\]", fields[2])
                    name = ("AMD " + m.group(1)) if m else fields[2]
            except (OSError, subprocess.SubprocessError):
                pass
    name = name or "AMD GPU (%s)" % card
    _name_cache[card] = name
    return name


def _amd_gpus() -> list[dict]:
    out = []
    cards = [p for p in glob.glob(os.path.join(DRM_ROOT, "card[0-9]*"))
             if re.search(r"card\d+$", p)]  # skip connectors like card0-DP-1
    for card_path in sorted(cards, key=_card_sort_key):
        dev = os.path.join(card_path, "device")
        if _read(os.path.join(dev, "vendor")) != "0x1002":
            continue
        total = _read_int(os.path.join(dev, "mem_info_vram_total")) or 0
        if total <= 0:
            continue  # APU without dedicated VRAM
        used = _read_int(os.path.join(dev, "mem_info_vram_used")) or 0
        busy = _read_int(os.path.join(dev, "gpu_busy_percent")) or 0
        card = os.path.basename(card_path)
        out.append({"id": card, "name": _amd_name(dev, card),
                    "total_mb": total / MIB, "used_mb": used / MIB,
                    "pct": float(max(0, min(100, busy)))})
    return out


def _nvidia_gpus() -> list[dict]:
    exe = shutil.which("nvidia-smi")
    if not exe:
        return []
    try:
        out = subprocess.run(
            [exe, "--query-gpu=index,name,utilization.gpu,memory.used,memory.total",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5).stdout
    except (OSError, subprocess.SubprocessError):
        return []
    gpus = []
    for ln in out.splitlines():
        parts = [p.strip() for p in ln.split(",")]
        if len(parts) != 5:
            continue
        try:
            gpus.append({"id": "nvidia%s" % parts[0], "name": parts[1],
                         "pct": float(parts[2]), "used_mb": float(parts[3]),
                         "total_mb": float(parts[4])})
        except ValueError:
            continue
    return gpus


def gpus() -> list[dict]:
    """[{id, name, total_mb, used_mb, pct}] for every GPU with dedicated VRAM,
    AMD first then NVIDIA, in a stable order. Cached for about a second."""
    now = time.monotonic()
    with _lock:
        if now - _gpu_cache["at"] < _GPU_TTL_S:
            return [dict(g) for g in _gpu_cache["gpus"]]
    found = _amd_gpus() + _nvidia_gpus()
    with _lock:
        _gpu_cache.update(at=now, gpus=found)
    return [dict(g) for g in found]


def gpu_engine_max() -> float:
    """Highest GPU utilisation (%) across all GPUs, 0 when none."""
    return max((g["pct"] for g in gpus()), default=0.0)


def gpu_vram_used_mb() -> float | None:
    """VRAM in use (MB), max across GPUs; None when there is no GPU."""
    g = gpus()
    return round(max(x["used_mb"] for x in g), 1) if g else None


def gpu_vram_total_mb() -> float | None:
    """Largest GPU's total VRAM (MB); None when there is no GPU."""
    g = gpus()
    return max(x["total_mb"] for x in g) if g else None


def process_vram_mb(pid: int) -> float | None:
    """VRAM (MB) used by one process: amdgpu DRM fdinfo (summed over distinct
    DRM clients), else nvidia-smi compute apps. None when unavailable."""
    if not pid:
        return None
    clients: dict[str, int] = {}
    for fd in glob.glob(os.path.join(PROC_ROOT, str(int(pid)), "fdinfo", "*")):
        txt = _read(fd) or ""
        if "drm-memory-vram" not in txt:
            continue
        cid = re.search(r"^drm-client-id:\s*(\d+)", txt, re.M)
        mem = re.search(r"^drm-memory-vram:\s*(\d+)\s*(\w+)", txt, re.M)
        if not mem:
            continue
        unit = {"KiB": 1024, "MiB": MIB, "GiB": 1024 * MIB}.get(mem.group(2), 1)
        clients[cid.group(1) if cid else fd] = int(mem.group(1)) * unit
    if clients:
        return round(sum(clients.values()) / MIB, 1)
    exe = shutil.which("nvidia-smi")
    if exe:
        try:
            out = subprocess.run(
                [exe, "--query-compute-apps=pid,used_memory",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5).stdout
            for ln in out.splitlines():
                p, _, mb = ln.partition(",")
                if p.strip() == str(pid):
                    return float(mb.strip())
        except (OSError, subprocess.SubprocessError, ValueError):
            pass
    return None


# ── Disks ────────────────────────────────────────────────────────────────────
_WHOLE_DISK = re.compile(r"^(sd[a-z]+|nvme\d+n\d+|vd[a-z]+|xvd[a-z]+|hd[a-z]+|mmcblk\d+)$")


def _whole_disks() -> list[str]:
    try:
        names = os.listdir(SYS_BLOCK)
    except OSError:
        return []
    return sorted(n for n in names if _WHOLE_DISK.match(n))


def disk_active_pct() -> list[tuple[str, float]]:
    """[(disk, active %)] for whole physical disks, from the busy-time delta
    since the previous call. The first call primes the counters and
    returns []."""
    if psutil is None:
        return []
    try:
        io = psutil.disk_io_counters(perdisk=True) or {}
    except Exception:
        return []
    now = time.monotonic()
    busy = {d: getattr(io[d], "busy_time", None) for d in _whole_disks() if d in io}
    with _lock:
        prev_t, prev = _disk_prev["t"], _disk_prev["busy"]
        _disk_prev.update(t=now, busy=busy)
    dt_ms = (now - prev_t) * 1000.0
    if not prev or dt_ms <= 0:
        return []
    out = []
    for d, b in busy.items():
        if b is None or prev.get(d) is None:
            continue
        out.append((d, round(max(0.0, min(100.0, (b - prev[d]) * 100.0 / dt_ms)), 1)))
    return out


def disk_inventory() -> dict:
    """disk name -> {model, letters, size}: `letters` holds the mount points of
    the disk's partitions (the Linux stand-in for drive letters)."""
    mounts: dict[str, list[str]] = {}
    if psutil is not None:
        try:
            for p in psutil.disk_partitions(all=False):
                dev = os.path.basename(p.device)
                for disk in _whole_disks():
                    if dev.startswith(disk):
                        mounts.setdefault(disk, []).append(p.mountpoint)
        except Exception:
            pass
    inv = {}
    for d in _whole_disks():
        sectors = _read_int(os.path.join(SYS_BLOCK, d, "size")) or 0
        inv[d] = {"model": (_read(os.path.join(SYS_BLOCK, d, "device", "model")) or d),
                  "letters": sorted(mounts.get(d, [])),
                  "size": sectors * 512}
    return inv


# ── RAM and processes ────────────────────────────────────────────────────────
def ram_total_mb() -> float:
    if psutil is None:
        return 0.0
    return psutil.virtual_memory().total / MIB


def ram_used_mb() -> float | None:
    """RAM in use (MB) = total - available, like `free`'s "used" column."""
    if psutil is None:
        return None
    vm = psutil.virtual_memory()
    return round((vm.total - vm.available) / MIB, 1)


def process_times(pid: int):
    """(wall_ms, cpu_ms) for a process, or None. Same contract as the Windows
    ProcessTimes.sample(): only deltas between two samples are meaningful."""
    if psutil is None:
        return None
    try:
        ct = psutil.Process(int(pid)).cpu_times()
    except Exception:
        return None
    return int(time.time() * 1000), int((ct.user + ct.system) * 1000)


def process_mem_io(pid: int):
    """(rss_bytes, io_read_bytes, io_write_bytes) for a process, or None."""
    if psutil is None:
        return None
    try:
        p = psutil.Process(int(pid))
        rss = int(p.memory_info().rss)
    except Exception:
        return None
    try:
        io = p.io_counters()
        return rss, int(io.read_bytes), int(io.write_bytes)
    except Exception:
        return rss, 0, 0


def find_pid(name: str) -> int | None:
    """First process whose executable name is `name` (e.g. llama-server)."""
    if psutil is None:
        return None
    for p in psutil.process_iter(["name", "cmdline"]):
        try:
            pname = p.info.get("name") or ""
            argv0 = os.path.basename((p.info.get("cmdline") or [""])[0])
        except Exception:
            continue
        if name in (pname, argv0):
            return p.pid
    return None


def kill_tree(pid: int) -> None:
    """Force-kill a process and its children (the taskkill /T /F stand-in)."""
    if psutil is None:
        return
    try:
        root = psutil.Process(int(pid))
        procs = root.children(recursive=True) + [root]
    except Exception:
        return
    for p in procs:
        try:
            p.kill()
        except Exception:
            pass
    psutil.wait_procs(procs, timeout=10)


# ── OpenCode Desktop sidecar ─────────────────────────────────────────────────
def _listen_ports(proc) -> list[int]:
    get = getattr(proc, "net_connections", None) or getattr(proc, "connections")
    try:
        conns = get(kind="tcp")
    except Exception:
        return []
    return [c.laddr.port for c in conns
            if c.status == psutil.CONN_LISTEN
            and c.laddr and c.laddr.ip in ("127.0.0.1", "::1")]


def desktop_sidecar():
    """(base_url, {username, password}) of a running OpenCode Desktop server,
    or None. The Desktop exposes per-launch Basic-auth credentials only in its
    process environment, so they are read fresh and never stored."""
    if psutil is None:
        return None
    for p in psutil.process_iter(["name"]):
        if "opencode" not in (p.info.get("name") or "").lower():
            continue
        try:
            env = p.environ()
        except Exception:
            continue
        user = env.get("OPENCODE_SERVER_USERNAME")
        pwd = env.get("OPENCODE_SERVER_PASSWORD")
        if not (user and pwd):
            continue
        try:
            family = [p] + p.children(recursive=True)
        except Exception:
            family = [p]
        for q in family:
            for port in _listen_ports(q):
                return "http://127.0.0.1:%d" % port, {"username": user, "password": pwd}
    return None
