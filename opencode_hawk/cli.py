"""`hawk` command line: one entry point for the monitor, the dashboard and setup."""
from __future__ import annotations

import argparse
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
        shutil.copyfile(PACKAGE_DIR / "config.example.json", cfg_path)
        print(f"created {cfg_path}")
    cfg = coordinator.load_config()
    topic = coordinator.ensure_ntfy_topic(cfg) or cfg["notify"]["ntfy"]["topic"]
    print(f"ntfy topic for this install: {topic}")
    print("  subscribe to it in the ntfy app to get phone alerts (treat it like a password)")
    print(f"next: set project_dir in {cfg_path}, then run `hawk run`")
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


def cmd_install_task(_args) -> int:
    from . import coordinator
    return coordinator.main(["--install-task"]) or 0


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
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)
