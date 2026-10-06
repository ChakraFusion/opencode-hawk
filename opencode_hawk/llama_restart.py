"""llama-restart service: periodic (time-based) + degradation-triggered restart
of the local llama-server process.

Why this exists (evidence, 2026-09-19):
- Decode/prefill speed falls monotonically with context depth (telemetry bins:
  22.9 t/s @5-10K -> 10.5 t/s @50-70K) because a 64K q8_0 KV pool spills
  ~4 GB into system RAM on a 16 GB card.
- With -np 1, parallel OpenCode sessions + compactions drive cache hits to 0
  and every request re-prefills 15-40K tokens, while the context keeps deep
  sessions in the slow region.
- A full restart resets both: measured good window after restart ~4 h
  (17-22 t/s), then decay toward 10-13 t/s.

Mitigation implemented here (all triggers union):
1. TIME  - restart every `llama_restart_cadence_min` (default 210 = 3.5 h) of
   uptime (from the last restart, or the process start time on first run).
2. DEEG - degradation signature from hawk telemetry: sustained low decode
   (avg tps < floor) while the context sits deep (avg tokens > depth floor)
   in the recent window.

The OpenCode plugin (`~/.config/opencode/plugins/llama-restart.ts`) asks us to
restart only at a session idle point right after a compaction; we additionally
refuse while llama slot 0 is busy, so no in-flight request is ever killed.

Respawn reuses the LIVE process cmdline/cwd/env (psutil), so server settings
never drift; a graceful HTTP /shutdown is attempted first, a forced kill
(taskkill / SIGKILL) only as the fallback.
"""

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

try:
    import psutil  # noqa: F401  (imported for restart execution)
except Exception:  # pragma: no cover - self-test env may lack psutil
    psutil = None

from . import coordinator  # same dir; run from coordinator/ or with it on sys.path

STATE_FN = "llama_restart.json"
LOCK_FN = "llama_restart.lock"
LOG_FN = "llama-server.out.log"

DEFAULTS = {
    # Planned restarts (not the dead-server respawn, which is always on) happen only when the user enabled them
    # (llama_kill_degraded) and the average decode speed is below llama_kill_tps. A time-based cadence is off (0)
    # unless set; it also needs llama_kill_degraded.
    "llama_restart_cadence_min": 0,
    "llama_restart_tps_floor": 14.0,
    "llama_restart_depth_floor": 30000,
    "llama_restart_window_s": 900,
    "llama_restart_min_active": 5,
    "llama_restart_wait_idle_s": 30,
    "llama_restart_force_after_s": 60,
    # After a forced kill: how long the process may take to exit (a server with a large host prompt cache
    # releases tens of GB first; 2026-10-05 it outlived a 2 s wait and the restart gave up with no server).
    "llama_restart_exit_wait_s": 120,
    "llama_restart_health_timeout_s": 240,
    "llama_restart_lock_ttl_s": 600,
    # Degradation-kill (webUI-configurable): when enabled, a sustained
    # slow-deep signature with avg tps below the threshold kills+respawns the
    # server even while it still answers /health.
    "llama_kill_degraded": False,
    "llama_kill_degraded_s": 300,
    "llama_kill_tps": 10.0,
}


# ── config helpers ───────────────────────────────────────────────────────────
def cfg_get(cfg, key):
    """Config value with a safe default (config.json may lack the key)."""
    v = cfg.get(key, DEFAULTS[key] if key in DEFAULTS else None)
    if v is None:
        return DEFAULTS[key]
    if isinstance(DEFAULTS[key], bool):
        # bool() of a str would coerce "false" to True; accept common spellings.
        return v if isinstance(v, bool) else str(v).strip().lower() in ("1", "true", "yes", "on")
    try:
        return type(DEFAULTS[key])(v)
    except (TypeError, ValueError):
        return DEFAULTS[key]


def _state_path() -> Path:
    return coordinator.home_dir() / STATE_FN


def _lock_path() -> Path:
    return coordinator.home_dir() / LOCK_FN


def load_state() -> dict:
    try:
        return json.loads(_state_path().read_text("utf-8"))
    except Exception:
        return {}


def save_state(d: dict) -> None:
    try:
        _state_path().write_text(json.dumps(d, indent=2), encoding="utf-8")
    except Exception as e:  # pragma: no cover
        coordinator.log_debug("llama_restart save_state: %s" % e)


def _now_ms() -> int:
    return int(time.time() * 1000)


# ── process helpers ──────────────────────────────────────────────────────────
def _pid(cfg):
    return coordinator.llama_pid(cfg)


def _process_spec(pid):
    """psutil capture of the live llama-server (argv list, cwd, env). Returns
    None when the process is gone or psutil is unavailable."""
    if not psutil:
        return None
    try:
        p = psutil.Process(pid)
        return {
            "argv": p.cmdline() or [],
            "cwd": p.cwd() or None,
            "env": dict(p.environ()),
            "started_ms": int(p.create_time() * 1000),
        }
    except Exception as e:
        coordinator.log_debug("llama_restart process_spec: %s" % e)
        return None


def _pid_alive(pid) -> bool:
    if not pid:
        return False
    if psutil:
        try:
            return psutil.pid_exists(pid) and psutil.Process(pid).status() != "zombie"
        except Exception:
            try:
                return psutil.pid_exists(pid)
            except Exception:
                return False
    try:
        out = subprocess.run(["tasklist", "/FI", "PID eq %d" % pid, "/FO",
                              "CSV", "/NH"],
                             capture_output=True, text=True, encoding="utf-8",
                             errors="replace", timeout=15,
                             creationflags=coordinator.creation_flags()).stdout
        return "llama" in out.lower() or str(pid) in out
    except Exception:
        return False


# ── liveness probe (cached tri-state) ───────────────────────────────────────
_HEALTH = {"at_ms": 0, "state": "down"}  # "ok" | "loading" | "down"


def health_state(cfg, max_age_ms=30_000) -> str:
    """Live HTTP /health probe, cached for max_age_ms.
    "ok" = 200 (serving), "loading" = 503 (model still loading),
    "down" = refused/timeout."""
    now = _now_ms()
    if 0 <= now - _HEALTH["at_ms"] < max_age_ms:
        return _HEALTH["state"]
    state = "down"
    try:
        with urllib.request.urlopen(coordinator.llama_api_base(cfg) + "/health",
                                    timeout=3) as r:
            state = "ok" if r.status == 200 else "loading"
    except urllib.error.HTTPError as e:
        state = "loading" if e.code == 503 else "down"
    except Exception:
        state = "down"
    _HEALTH.update({"at_ms": now, "state": state})
    return state


# ── degradation backstop (pure, testable) ───────────────────────────────────
def degraded_signal(cfg, db_path=None) -> dict:
    """Query hawk telemetry for the proven slow-deep signature over the recent
    window (active decode samples only). Pure DB read; no network.
    Returns {'degraded': bool, 'active': int, 'avg_tps': float, 'avg_depth': float}."""
    window_s = cfg_get(cfg, "llama_restart_window_s")
    floor = cfg_get(cfg, "llama_restart_tps_floor")
    depth = cfg_get(cfg, "llama_restart_depth_floor")
    min_active = cfg_get(cfg, "llama_restart_min_active")
    out = {"degraded": False, "active": 0, "avg_tps": 0.0, "avg_depth": 0.0}
    if db_path is None:
        db_path = coordinator.home_dir() / "hawk_telemetry.db"
    db_path = Path(db_path)
    if not db_path.exists():
        return out
    import sqlite3
    try:
        con = sqlite3.connect("file:%s?mode=ro" % str(db_path), uri=True,
                              timeout=5)
        try:
            since = _now_ms() - window_s * 1000
            row = con.execute(
                "SELECT COUNT(*), AVG(tps), AVG(tokens) FROM telemetry "
                "WHERE ts >= ? AND tps > 0", (since,)).fetchone()
        finally:
            con.close()
        n = int(row[0] or 0)
        avg_tps = float(row[1] or 0.0)
        avg_depth = float(row[2] or 0.0)
        out["active"] = n
        out["avg_tps"] = round(avg_tps, 2)
        out["avg_depth"] = round(avg_depth, 0)
        if n >= min_active and avg_tps < floor and avg_depth > depth:
            out["degraded"] = True
    except Exception as e:
        coordinator.log_debug("llama_restart degraded_signal: %s" % e)
    return out


# ── due computation (pure, testable) ────────────────────────────────────────
def due_reason(now_ms: int, last_at_ms, cadence_s, degraded: bool) -> dict:
    """Union of time + degradation triggers.
    Returns {'due': bool, 'reasons': [str]}."""
    reasons = []
    if cadence_s and last_at_ms and (now_ms - last_at_ms) >= cadence_s * 1000:
        reasons.append("time")
    if degraded:
        reasons.append("degraded")
    return {"due": bool(reasons), "reasons": reasons}


def planned_due(cfg, now_ms: int, last_at_ms, deg: dict) -> dict:
    """Planned restart: only when enabled (llama_kill_degraded) and the average decode speed is below
    llama_kill_tps (or, if configured, the uptime cadence ran out). Disabled -> never due; a dead or unhealthy
    server is handled separately by status() and always restarted."""
    if not cfg_get(cfg, "llama_kill_degraded"):
        return {"due": False, "reasons": []}
    tps = deg.get("avg_tps")
    slow = bool(deg.get("degraded")) and tps is not None and tps < cfg_get(cfg, "llama_kill_tps")
    return due_reason(now_ms, last_at_ms, cfg_get(cfg, "llama_restart_cadence_min") * 60, slow)


def kill_decision(cfg, deg: dict, deg_since_ms, now_ms: int) -> dict:
    """Decide the degradation-kill (webUI 'Kill llama when degraded').

    Enabled via `llama_kill_degraded`; fires only while the degraded signature
    from degraded_signal() (slow + deep) stays true AND the sustained avg tps
    is below `llama_kill_tps` for `llama_kill_degraded_s` seconds. Any healthy
    sample (not degraded, or tps at/above the threshold) resets the streak.

    Args:
      deg          - degraded_signal() result: {degraded, avg_tps, ...}
      deg_since_ms - first-seen timestamp from the previous tick (state file),
                     or None when the streak is new/cleared.
      now_ms       - current epoch ms.
    Returns {'kill': bool, 'state': str, 'degraded_since_ms': int|None}.
      state 'disabled'|'clear' -> timer reset, no kill
            'wait'             -> streak started/counting
            'armed'            -> sustained past the time -> kill
    """
    if not cfg_get(cfg, "llama_kill_degraded"):
        return {"kill": False, "state": "disabled", "degraded_since_ms": None}
    if not deg.get("degraded") or deg.get("avg_tps", 0.0) >= cfg_get(cfg, "llama_kill_tps"):
        return {"kill": False, "state": "clear", "degraded_since_ms": None}
    if deg_since_ms is None:
        return {"kill": False, "state": "wait",
                "degraded_since_ms": now_ms}
    sustained = (now_ms - deg_since_ms) >= cfg_get(cfg, "llama_kill_degraded_s") * 1000
    return {"kill": sustained, "state": "armed" if sustained else "wait",
            "degraded_since_ms": deg_since_ms}


# ── status (read-only, safe to call anytime) ───────────────────────────────
def status(cfg) -> dict:
    st = load_state()
    last = st.get("last_at_ms")
    pid = _pid(cfg)
    spec = _process_spec(pid) if pid else None
    if not last and spec:
        last = spec["started_ms"]  # first run: treat process start as baseline
    now = _now_ms()
    cad = cfg_get(cfg, "llama_restart_cadence_min") * 60
    deg = degraded_signal(cfg)
    due = planned_due(cfg, now, last, deg)
    # Liveness-validated state: a dead pid must read as restart_due even when
    # the state file says healthy (the 2026-09-25 16:21 gap); an alive pid
    # whose /health probe refuses is treated the same (half-dead survivor).
    # 503 (model loading) is informational only and never forces a restart.
    alive = _pid_alive(pid) if pid else False
    hstate = health_state(cfg) if alive else "down"
    if not alive:
        due["due"] = True
        if "dead" not in due["reasons"]:
            due["reasons"] = ["dead"] + due["reasons"]
    elif hstate == "down":
        due["due"] = True
        if "unhealthy" not in due["reasons"]:
            due["reasons"] = ["unhealthy"] + due["reasons"]
    elif hstate == "loading" and "loading" not in due["reasons"]:
        due["reasons"] = ["loading"] + due["reasons"]
    slots = coordinator.llama_slots(cfg)
    locked = _lock_held()
    kill = {
        "kill_degraded": bool(cfg_get(cfg, "llama_kill_degraded")),
        "kill_degraded_s": cfg_get(cfg, "llama_kill_degraded_s"),
        "kill_tps": cfg_get(cfg, "llama_kill_tps"),
        "degraded_since_ms": st.get("degraded_since_ms"),
    }
    return {
        "restart_due": bool(due["due"]),
        "reasons": due["reasons"],
        "last_restart_at_ms": last,
        "uptime_s": (now - last) / 1000 if last else None,
        "cadence_min": cfg_get(cfg, "llama_restart_cadence_min"),
        "pid": pid,
        "alive": alive,
        "health": hstate,
        "degraded": deg["degraded"],
        "degraded_active": deg["active"],
        "degraded_avg_tps": deg["avg_tps"],
        "degraded_avg_depth": deg["avg_depth"],
        "slots_idle": None if slots is None else (not slots["is_processing"]),
        "locked": locked,
        "kill": kill,
        "now_ms": now,
    }


# ── lock ────────────────────────────────────────────────────────────────────
def _lock_held() -> bool:
    p = _lock_path()
    if not p.exists():
        return False
    try:
        age = _now_ms() - int(json.loads(p.read_text("utf-8")).get("at_ms", 0))
        return age < cfg_get(coordinator.load_config(), "llama_restart_lock_ttl_s") * 1000
    except Exception:
        return False


def _lock(by: str) -> bool:
    """Claim the restart lock. Returns True when acquired."""
    if _lock_held():
        return False
    try:
        _lock_path().write_text(json.dumps({"at_ms": _now_ms(), "by": by}),
                                encoding="utf-8")
        return True
    except Exception:
        return False


def _unlock() -> None:
    try:
        _lock_path().unlink()
    except Exception:
        pass


# ── execution ───────────────────────────────────────────────────────────────
def _graceful_shutdown(cfg, pid) -> bool:
    """POST /shutdown (GET fallback), then wait for the process to exit; force
    taskkill after `force_after_s` seconds. Returns True when the process is
    gone."""
    base = coordinator.llama_api_base(cfg)
    force_after = cfg_get(cfg, "llama_restart_force_after_s")
    accepted = False
    for method in ("POST", "GET"):
        try:
            req = urllib.request.Request(base + "/shutdown", method=method)
            urllib.request.urlopen(req, timeout=5)
            accepted = True
            break
        except urllib.error.HTTPError:
            break  # the server answered but has no /shutdown (llama-server): no point waiting for it
        except Exception:
            continue
    deadline = time.time() + (force_after if accepted else 0)
    while time.time() < deadline:
        if not _pid_alive(pid):
            return True
        time.sleep(1.0)
    if _pid_alive(pid):  # force fallback (logged; should rarely trigger)
        coordinator.log_debug("llama_restart: graceful shutdown timed out, "
                              "taskkill /F %d" % pid)
        try:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                               capture_output=True, timeout=15,
                               creationflags=coordinator.creation_flags())
            else:
                from . import hawk_linux
                hawk_linux.kill_tree(pid)
        except Exception as e:
            coordinator.log_debug("llama_restart taskkill: %s" % e)
        exit_deadline = time.time() + cfg_get(cfg, "llama_restart_exit_wait_s")
        while _pid_alive(pid) and time.time() < exit_deadline:
            time.sleep(1.0)
    return not _pid_alive(pid)


def _respawn(spec) -> int | None:
    """Spawn llama-server from the captured spec; returns the new pid."""
    if not spec or not spec.get("argv"):
        return None
    log_path = coordinator.home_dir() / LOG_FN
    try:
        fh = open(log_path, "ab")
    except Exception:
        fh = None
    try:
        proc = coordinator.spawn_independent(
            spec["argv"],
            cwd=spec.get("cwd") or None,
            env=spec.get("env") or None,
            stdout=fh, stderr=subprocess.STDOUT,
        )
        from . import probes
        probes.record_llama_spawn(proc.pid)
        return proc.pid
    except Exception as e:
        coordinator.log_debug("llama_restart respawn: %s" % e)
        return None
    finally:
        if fh:
            try:
                fh.close()
            except Exception:
                pass


def _wait_healthy(cfg, timeout_s: int | None = None) -> bool:
    """Poll /health until 200 (llama-server 503s while the model loads)."""
    if timeout_s is None:
        timeout_s = cfg_get(cfg, "llama_restart_health_timeout_s")
    base = coordinator.llama_api_base(cfg)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(base + "/health", timeout=2) as r:
                if r.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(2.0)
    return False


def restart(cfg, reason: str = "manual") -> dict:
    """Full restart sequence. Safe-guards: refuses when llama is gone, when a
    restart is already in flight, while slot 0 is busy (waits up to
    `wait_idle_s`), and only shuts down gracefully first."""
    pid = _pid(cfg)
    if not pid:
        return {"ok": False, "reason": "no_llama_process"}
    if not _lock("restart:%s" % reason):
        return {"ok": False, "reason": "locked"}
    try:
        spec = _process_spec(pid)
        if not spec or not spec.get("argv"):
            return {"ok": False, "reason": "no_process_spec"}
        # Refuse while the slot is busy (no in-flight request killed).
        wait_idle = cfg_get(cfg, "llama_restart_wait_idle_s")
        idle_deadline = time.time() + wait_idle
        while time.time() < idle_deadline:
            slots = coordinator.llama_slots(cfg)
            if slots is None or not slots["is_processing"]:
                break
            time.sleep(1.0)
        else:
            return {"ok": False, "reason": "busy"}
        started = _now_ms()
        ok_shutdown = _graceful_shutdown(cfg, pid)
        if not ok_shutdown and _pid_alive(pid):
            return {"ok": False, "reason": "shutdown_failed"}  # still running: nothing lost
        # From here the server is gone: whatever happens, never leave it down. One start at a time (llama-start).
        if not coordinator.hold_lock("llama-start"):
            return {"ok": False, "reason": "another start in progress"}
        try:
            new_pid = None if coordinator.llama_server_alive(cfg) else _respawn(spec)
        finally:
            coordinator.release_lock("llama-start")
        if not new_pid:
            from . import probes
            if probes.llama_respawn(cfg):
                return {"ok": True, "healthy": True, "pid_old": pid, "pid_new": coordinator.llama_pid(cfg),
                        "reason": reason, "via": "llama_bat_path"}
            return {"ok": False, "reason": "respawn_failed"}
        healthy = _wait_healthy(cfg)
        st = {
            "last_at_ms": _now_ms(),
            "pid_old": pid,
            "pid_new": new_pid,
            "ok": bool(healthy),
            "healthy": bool(healthy),
            "reason": reason,
            "elapsed_ms": _now_ms() - started,
            "graceful": ok_shutdown,
        }
        save_state(st)
        return {"ok": True, "healthy": healthy, "pid_old": pid,
                "pid_new": new_pid, "elapsed_ms": st["elapsed_ms"],
                "reason": reason}
    finally:
        _unlock()


# ── supervision (monitor-loop liveness watchdog) ────────────────────────────
def _supervise_action(due: bool, alive: bool, hstate: str) -> str:
    """Pure decision table for the monitor supervisor. Returns:
      none      - not due (time/degradation triggers not met)
      healthy   - due but the server is alive AND serving -> do nothing
                  (cadence restarts stay with the plugin's idle points)
      loading   - alive, /health 503 -> do nothing (model loading)
      restart   - alive but unhealthy -> supervised restart (kill+respawn)
      respawn   - process gone -> spawn from the live cmdline/bat
    """
    if not due:
        return "none"
    if alive and hstate == "ok":
        return "healthy"
    if alive and hstate == "loading":
        return "loading"
    if alive:
        return "restart"
    return "respawn"


def supervise(cfg) -> dict:
    """Monitor-loop watchdog: act when restart_due AND liveness is failing.

    Closes two historical gaps (2026-09-25 incident: server died ~16:21 and
    was not restarted until 17:14):
    - restart_due was computed but no executor existed: the only callers were
      the opencode plugin at idle-after-compaction and a buried branch of the
      session self-check that early-returns on busy sessions, so a dead
      server behind an unfinished turn was never restarted;
    - a dead process with a "healthy": true state file read as NOT due.
    """
    st = status(cfg)
    # Degradation-kill pass (webUI toggle): a healthy-but-slow server is never
    # touched by the liveness actions below, so handle the kill here.
    stfile = load_state()
    kd = kill_decision(cfg, st, stfile.get("degraded_since_ms"), _now_ms())
    st["kill"] = kd
    if kd["kill"]:
        c2 = dict(cfg)
        c2["llama_restart_health_timeout_s"] = min(
            int(cfg_get(cfg, "llama_restart_health_timeout_s") or 240), 30)
        res = restart(c2, "degraded")
        st.update(res)
        st["action"] = "restarted" if res.get("ok") else "restart_failed"
        # restart() saves a fresh state on success (clearing degraded_since_ms);
        # on failure the streak stays armed and the next tick retries.
        return st
    nf = dict(stfile)
    if kd["state"] in ("disabled", "clear"):
        if nf.get("degraded_since_ms") is not None:
            nf["degraded_since_ms"] = None
            save_state(nf)
    elif kd["state"] == "wait":
        if nf.get("degraded_since_ms") is None:
            nf["degraded_since_ms"] = kd["degraded_since_ms"]
            save_state(nf)
    action = _supervise_action(st["restart_due"], st.get("alive", False),
                               st.get("health", "down"))
    st["action"] = action
    if action in ("none", "healthy", "loading"):
        return st
    if action == "restart":
        # Alive but unhealthy: supervised restart. Shrink the health wait so
        # the monitor never stalls on it; the next tick re-checks.
        c2 = dict(cfg)
        c2["llama_restart_health_timeout_s"] = min(
            int(cfg_get(cfg, "llama_restart_health_timeout_s") or 240), 30)
        res = restart(c2, "supervised")
        st.update(res)
        st["action"] = "restarted" if res.get("ok") else "restart_failed"
        return st
    # respawn: process gone. Go through coordinator.llama_respawn (canonical
    # log capture; no-op when a server is somehow already up), lock-guarded
    # so a concurrent in-flow llama_down can never double-spawn.
    if not _lock("supervise:respawn"):
        st["action"] = "locked"
        return st
    try:
        started = _now_ms()
        old_pid = st.get("pid")
        ok = coordinator.llama_respawn(cfg, timeout_s=30)
        new_pid = coordinator.llama_pid(cfg)
        st.update(status(cfg))
        st["action"] = "respawned" if ok else "respawn_failed"
        if ok:
            save_state({
                "last_at_ms": _now_ms(),
                "pid_old": old_pid,
                "pid_new": new_pid,
                "ok": True,
                "healthy": True,
                "reason": "supervised",
                "elapsed_ms": _now_ms() - started,
                "graceful": False,
            })
    finally:
        _unlock()
    return st


if __name__ == "__main__":  # pragma: no cover - manual debug entry
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--self-test":
        cases = [
            ("supervise_none", _supervise_action(False, True, "ok") == "none"),
            ("supervise_healthy", _supervise_action(True, True, "ok") == "healthy"),
            ("supervise_loading", _supervise_action(True, True, "loading") == "loading"),
            ("supervise_restart", _supervise_action(True, True, "down") == "restart"),
            ("supervise_respawn", _supervise_action(True, False, "down") == "respawn"),
        ]
        for name, cond in cases:
            print(("PASS  " if cond else "FAIL  ") + name)
        sys.exit(0 if all(c for _, c in cases) else 1)
    c = coordinator.load_config()
    if len(sys.argv) > 1 and sys.argv[1] == "--status":
        print(json.dumps(status(c), indent=2))
    else:
        print(json.dumps(restart(c, "manual"), indent=2))