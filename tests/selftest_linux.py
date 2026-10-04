#!/usr/bin/env python3
"""Self-test for hawk_linux: the Linux GPU / disk / VRAM parsers run against a
fake sysfs and procfs tree, so they are exercised on every OS (CI runners
have no GPU). Live psutil-backed probes are checked for shape only.

Run:  python tests/selftest_linux.py     (exit 0 = all pass)
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import hawk_linux as hl  # noqa: E402

FAILS = []


def check(name, cond):
    print("%-30s %s" % (name, "OK" if cond else "FAIL"))
    if not cond:
        FAILS.append(name)


def write(root: Path, rel: str, text: str):
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def main() -> int:
    tmp = Path(tempfile.mkdtemp())
    saved = (hl.DRM_ROOT, hl.SYS_BLOCK, hl.PROC_ROOT)
    saved_which = hl.shutil.which
    try:
        drm, blk, proc = tmp / "drm", tmp / "block", tmp / "proc"
        # card0: AMD dGPU with 16 GB VRAM, 37 % busy
        write(drm, "card0/device/vendor", "0x1002\n")
        write(drm, "card0/device/product_name", "AMD Radeon RX 9060 XT\n")
        write(drm, "card0/device/mem_info_vram_total", str(16304 * hl.MIB))
        write(drm, "card0/device/mem_info_vram_used", str(4096 * hl.MIB))
        write(drm, "card0/device/gpu_busy_percent", "37\n")
        # card0-DP-1: a connector, must be ignored
        write(drm, "card0-DP-1/status", "connected\n")
        # card1: AMD APU without dedicated VRAM, must be ignored
        write(drm, "card1/device/vendor", "0x1002\n")
        write(drm, "card1/device/mem_info_vram_total", "0")
        # card2: Intel iGPU, must be ignored
        write(drm, "card2/device/vendor", "0x8086\n")
        # block devices: two whole disks, plus loop/ram devices to skip
        write(blk, "nvme0n1/size", str(1_000_000_000 // 512))
        write(blk, "nvme0n1/device/model", "Fast NVMe\n")
        write(blk, "sda/size", str(2_000_000_000_000 // 512))
        write(blk, "loop0/size", "100")
        write(blk, "ram0/size", "100")
        # llama process 4242: two DRM clients (one listed twice) holding VRAM
        write(proc, "4242/fdinfo/5", "pos:\t0\ndrm-driver:\tamdgpu\ndrm-client-id:\t7\n"
                                     "drm-memory-vram:\t2097152 KiB\n")
        write(proc, "4242/fdinfo/6", "pos:\t0\ndrm-driver:\tamdgpu\ndrm-client-id:\t7\n"
                                     "drm-memory-vram:\t2097152 KiB\n")
        write(proc, "4242/fdinfo/9", "pos:\t0\ndrm-driver:\tamdgpu\ndrm-client-id:\t8\n"
                                     "drm-memory-vram:\t1024 MiB\n")
        write(proc, "4242/fdinfo/1", "pos:\t0\nflags:\t02\n")

        hl.DRM_ROOT, hl.SYS_BLOCK, hl.PROC_ROOT = str(drm), str(blk), str(proc)
        hl.shutil.which = lambda name: None  # no nvidia-smi / lspci
        hl._gpu_cache.update(at=0.0, gpus=[])
        hl._name_cache.clear()

        g = hl.gpus()
        check("amd_one_gpu", len(g) == 1)
        check("amd_fields", g and g[0]["id"] == "card0"
              and g[0]["name"] == "AMD Radeon RX 9060 XT"
              and abs(g[0]["total_mb"] - 16304) < 0.01
              and abs(g[0]["used_mb"] - 4096) < 0.01 and g[0]["pct"] == 37.0)
        check("engine_max", hl.gpu_engine_max() == 37.0)
        check("vram_used", hl.gpu_vram_used_mb() == 4096.0)
        check("vram_total", abs(hl.gpu_vram_total_mb() - 16304) < 0.01)
        check("gpus_cached_copy", hl.gpus() == g and hl.gpus() is not g)

        # name fallback when the driver has no product_name and lspci is absent
        (drm / "card0/device/product_name").unlink()
        hl._name_cache.clear()
        hl._gpu_cache.update(at=0.0)
        check("amd_name_fallback", hl.gpus()[0]["name"] == "AMD GPU (card0)")

        # per-process VRAM: client 7 counted once (2 GiB) + client 8 (1 GiB)
        check("proc_vram_fdinfo", hl.process_vram_mb(4242) == 3072.0)
        check("proc_vram_missing", hl.process_vram_mb(999999) is None)
        check("proc_vram_no_pid", hl.process_vram_mb(0) is None)

        # whole disks only
        check("whole_disks", hl._whole_disks() == ["nvme0n1", "sda"])
        inv = hl.disk_inventory()
        check("disk_inventory", set(inv) == {"nvme0n1", "sda"}
              and inv["nvme0n1"]["model"] == "Fast NVMe"
              and inv["sda"]["size"] == 2_000_000_000_000 // 512 * 512)

        # no GPU at all
        shutil.rmtree(drm)
        drm.mkdir()
        hl._gpu_cache.update(at=0.0)
        check("no_gpu_empty", hl.gpus() == [] and hl.gpu_engine_max() == 0.0)
        check("no_gpu_none", hl.gpu_vram_used_mb() is None
              and hl.gpu_vram_total_mb() is None)
    finally:
        hl.DRM_ROOT, hl.SYS_BLOCK, hl.PROC_ROOT = saved
        hl.shutil.which = saved_which
        hl._gpu_cache.update(at=0.0, gpus=[])
        shutil.rmtree(tmp, ignore_errors=True)

    # live psutil probes (any OS): shape only
    check("ram_total", hl.ram_total_mb() > 0)
    used = hl.ram_used_mb()
    check("ram_used", used is not None and 0 < used <= hl.ram_total_mb())
    pt = hl.process_times(os.getpid())
    check("process_times", isinstance(pt, tuple) and len(pt) == 2 and pt[1] >= 0)
    mio = hl.process_mem_io(os.getpid())
    check("process_mem_io", isinstance(mio, tuple) and len(mio) == 3 and mio[0] > 0)
    check("process_times_dead", hl.process_times(2 ** 30) is None)
    check("find_pid_none", hl.find_pid("no-such-process-hawk") is None)

    print()
    print("linux self-test: %s" % ("PASS" if not FAILS else "FAIL (%s)" % ", ".join(FAILS)))
    return 0 if not FAILS else 1


if __name__ == "__main__":
    sys.exit(main())
