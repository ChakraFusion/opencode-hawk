"""`hawk` command line: one entry point for the monitor, the dashboard and setup."""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

from . import __version__

PACKAGE_DIR = Path(__file__).resolve().parent


def cmd_init(_args) -> int:
    from . import coordinator
    home = coordinator.home_dir()
    cfg_path = home / "config.json"
    if cfg_path.exists():
        print(f"config already exists: {cfg_path}")
    else:
        home.mkdir(parents=True, exist_ok=True)  # a fresh install: %APPDATA%\opencode-hawk does not exist yet
        shutil.copyfile(PACKAGE_DIR / "config.example.json", cfg_path)
        print(f"created {cfg_path}")
    cfg = coordinator.load_config()
    topic = coordinator.ensure_ntfy_topic(cfg) or cfg["notify"]["ntfy"]["topic"]
    print(f"ntfy topic for this install: {topic}")
    print("  subscribe to it in the ntfy app to get phone alerts (treat it like a password)")
    print(f"next: set project_dir and llama_bat_path (the script that starts llama-server) in {cfg_path},")
    print("      then run `hawk run`")
    return 0


def cmd_where(_args) -> int:
    from . import coordinator
    home = coordinator.home_dir()
    print(f"home:      {home}")
    print(f"config:    {home / 'config.json'}{'' if (home / 'config.json').exists() else '  (missing: run `hawk init`)'}")
    print(f"opencode:  {coordinator.db_path()}")
    return 0


def _monitor_argv(args, once: bool) -> list[str]:
    argv = ["--once"] if once else ["--monitor"]
    if not once and getattr(args, "interval", None):
        argv += ["--interval", str(args.interval)]
    if args.dry_run:
        argv.append("--dry-run")
    if args.session_id:
        argv += ["--session-id", args.session_id]
    return argv


def cmd_monitor(args) -> int:
    from . import coordinator
    return coordinator.main(_monitor_argv(args, once=False)) or 0


def cmd_poll(args) -> int:
    from . import coordinator
    return coordinator.main(_monitor_argv(args, once=True)) or 0


def cmd_dashboard(args) -> int:
    from . import dashboard
    argv = ["--port", str(args.port), "--listen", args.listen] + (["--open"] if args.open else [])
    return dashboard.main(argv) or 0


PLUGIN_NAME = "local-hawk-llama-restart.ts"


def opencode_plugin_dir() -> Path:
    """OpenCode's global plugin folder: $OPENCODE_CONFIG_DIR/plugins, else ~/.config/opencode/plugins."""
    import os
    base = os.environ.get("OPENCODE_CONFIG_DIR")
    return (Path(base) if base else Path.home() / ".config" / "opencode") / "plugins"


def cmd_install_plugin(args) -> int:
    """Install (or remove) Hawk's llama-restart plugin into OpenCode. The local-* name keeps it out of a harness
    that is a git repository (machine-local plugins are git-ignored there)."""
    target_dir = Path(args.dir) if args.dir else opencode_plugin_dir()
    target = target_dir / PLUGIN_NAME
    if args.remove:
        if target.exists():
            target.unlink()
            print(f"removed {target}; restart OpenCode to unload it")
        else:
            print(f"not installed: {target}")
        return 0
    target_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(PACKAGE_DIR / "opencode-plugin" / "llama-restart.ts", target)
    print(f"installed {target}; restart OpenCode to load it")
    if (target_dir / "llama-restart.ts").exists():
        print(f"warning: {target_dir / 'llama-restart.ts'} is an older copy of this plugin; remove it, or every "
              "restart is requested twice")
    return 0


def cmd_install_task(_args) -> int:
    from . import coordinator
    return coordinator.main(["--install-task"]) or 0


WATCHDOG_TASK = "HawkWatchdog"


def _hawk_processes():
    """Running Hawk monitor / dashboard processes (psutil), as {'monitor': [pids], 'dashboard': [pids], 'run': [pids]}."""
    import psutil
    found = {"monitor": [], "dashboard": [], "run": []}
    for p in psutil.process_iter(["pid", "cmdline"]):
        argv = [str(a) for a in (p.info.get("cmdline") or [])]
        if p.info["pid"] == os.getpid() or not any("opencode_hawk" in a or a.lower().endswith(("hawk", "hawk.exe"))
                                                    for a in argv):
            continue
        for role in ("monitor", "dashboard", "run"):
            if role in argv:
                found[role].append(p.info["pid"])
    return found


def _url_ok(url: str) -> bool:
    import urllib.request
    try:
        with urllib.request.urlopen(url, timeout=4) as r:
            return 200 <= r.status < 500
    except Exception:
        return False


def cmd_ensure(args) -> int:
    """Failsafe (run every few minutes by `hawk install-watchdog`): a stopped Hawk is started again, outside any
    app's job; a llama-server that stays down across two checks while Hawk runs is started via llama_bat_path.
    2026-10-05: Hawk and llama-server died with the app that had started them, and the night's run stopped."""
    import json
    from . import coordinator
    home = coordinator.home_dir()
    procs = _hawk_processes()
    dash_ok = _url_ok("http://127.0.0.1:%d/" % args.port)
    if not procs["monitor"] or not dash_ok:
        import psutil
        for pid in procs["monitor"] + procs["dashboard"] + procs["run"]:
            try:
                psutil.Process(pid).kill()  # a half-running Hawk: start clean, never twice
            except Exception:
                pass
        log = open(home / "hawk-console.log", "ab")
        coordinator.spawn_independent([sys.executable, "-m", "opencode_hawk", "run", "--no-open", "--port", str(args.port)],
                                      stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        coordinator.log("WATCHDOG: Hawk was not running (monitor %s, dashboard %s); started it"
                        % ("up" if procs["monitor"] else "down", "up" if dash_ok else "down"))
        return 0
    cfg = coordinator.load_config()
    state_file = home / "watchdog.json"
    try:
        state = json.loads(state_file.read_text(encoding="utf-8"))
    except Exception:
        state = {}
    llama_ok = _url_ok(coordinator.llama_api_base(cfg) + "/health")
    misses = 0 if llama_ok else int(state.get("llama_misses") or 0) + 1
    if misses >= 2 and (cfg.get("llama_bat_path") or "").strip():
        coordinator.log("WATCHDOG: llama-server down for %d checks while Hawk runs; starting it" % misses)
        if coordinator.llama_respawn(cfg):
            misses = 0
    state_file.write_text(json.dumps({"llama_misses": misses}), encoding="utf-8")
    return 0


def cmd_install_watchdog(args) -> int:
    """Register `hawk ensure` with the OS scheduler: Task Scheduler on Windows (pythonw, no window), cron elsewhere."""
    if os.name != "nt":
        print("# add to `crontab -e`:")
        print("*/%d * * * * %s -m opencode_hawk ensure" % (args.every, sys.executable))
        return 0
    if args.remove:
        r = subprocess.run(["schtasks", "/Delete", "/F", "/TN", WATCHDOG_TASK], capture_output=True, text=True)
        print((r.stdout or r.stderr).strip())
        return r.returncode
    pyw = Path(sys.executable).with_name("pythonw.exe")
    exe = pyw if pyw.exists() else Path(sys.executable)
    tr = '"%s" -m opencode_hawk ensure' % exe
    r = subprocess.run(["schtasks", "/Create", "/F", "/TN", WATCHDOG_TASK, "/SC", "MINUTE", "/MO", str(args.every),
                        "/TR", tr, "/RL", "LIMITED"], capture_output=True, text=True)
    print((r.stdout or r.stderr).strip())
    if r.returncode == 0:
        print("watchdog: every %d min `hawk ensure` starts Hawk if it is down (remove: hawk install-watchdog --remove)"
              % args.every)
    return r.returncode


def cmd_run(args) -> int:
    """Monitor and dashboard as two child processes; Ctrl+C stops both."""
    py = [sys.executable, "-m", "opencode_hawk"]
    monitor = py + ["monitor", "--interval", str(args.interval)]
    dash = py + ["dashboard", "--port", str(args.port)] + ([] if args.no_open else ["--open"])
    procs = [subprocess.Popen(monitor), subprocess.Popen(dash)]
    print(f"Hawk is running: dashboard http://127.0.0.1:{args.port} (Ctrl+C stops it)")
    try:
        while all(p.poll() is None for p in procs):
            try:
                procs[0].wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
    return next((p.returncode for p in procs if p.returncode not in (None, 0)), 0)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="hawk", description="Hawk: keeps multi-agent OpenCode workflows moving.")
    ap.add_argument("--version", action="version", version=f"hawk {__version__}")
    sub = ap.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="create config.json and a private ntfy topic").set_defaults(func=cmd_init)
    sub.add_parser("where", help="show the folders Hawk uses").set_defaults(func=cmd_where)

    p = sub.add_parser("run", help="start monitor and dashboard (Ctrl+C stops both)")
    p.add_argument("--interval", type=int, default=3, help="monitor interval in minutes (default 3)")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--no-open", action="store_true", help="do not open the browser")
    p.set_defaults(func=cmd_run)

    for name, func, helptext in (("monitor", cmd_monitor, "the poll loop only"),
                                 ("poll", cmd_poll, "one poll pass (for Task Scheduler / cron)")):
        p = sub.add_parser(name, help=helptext)
        if name == "monitor":
            p.add_argument("--interval", type=int, default=None, help="minutes between polls")
        p.add_argument("--dry-run", action="store_true", help="evaluate without sending or saving state")
        p.add_argument("--session-id", default=None, help="watch this session instead of the configured one")
        p.set_defaults(func=func)

    p = sub.add_parser("dashboard", help="the web dashboard only")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--listen", default="127.0.0.1")
    p.add_argument("--open", action="store_true", help="open the browser")
    p.set_defaults(func=cmd_dashboard)

    sub.add_parser("install-task", help="print the Task Scheduler / cron entry").set_defaults(func=cmd_install_task)

    p = sub.add_parser("ensure", help="failsafe: start Hawk if it is not running; start llama-server if it stays down")
    p.add_argument("--port", type=int, default=8765)
    p.set_defaults(func=cmd_ensure)

    p = sub.add_parser("install-watchdog", help="run `hawk ensure` every 2 minutes (Task Scheduler / cron)")
    p.add_argument("--remove", action="store_true", help="remove the watchdog task")
    p.add_argument("--every", type=int, default=2, help="minutes between checks (default 2)")
    p.set_defaults(func=cmd_install_watchdog)

    p = sub.add_parser("install-plugin", help="install the llama-restart plugin into OpenCode")
    p.add_argument("--dir", default=None, help="plugin folder (default: OpenCode's global plugins folder)")
    p.add_argument("--remove", action="store_true", help="uninstall it")
    p.set_defaults(func=cmd_install_plugin)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)
