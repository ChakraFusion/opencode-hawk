"""`hawk setup`: a guided setup, so nobody has to edit config.json by hand.

Steps: config + private ntfy topic, llama-server (detected; a start script is written from the running server's
exact command line when none is set), project folder, the failsafe watchdog, the optional OpenCode plugin, and
starting Hawk. Every question has a default; `--yes` takes all of them.
"""
from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path


def _say(msg: str = "") -> None:
    print(msg, flush=True)


class Asker:
    """Questions with defaults. With assume_yes, every default is taken; answers can be scripted (tests)."""

    def __init__(self, assume_yes=False, answers=None):
        self.assume_yes = assume_yes
        self.answers = list(answers or [])

    def text(self, question: str, default: str = "") -> str:
        if self.answers:
            a = self.answers.pop(0)
        elif self.assume_yes:
            a = ""
        else:
            a = input("%s%s: " % (question, " [%s]" % default if default else "")).strip()
        return a or default

    def yes(self, question: str, default: bool = True) -> bool:
        a = self.text("%s (%s)" % (question, "Y/n" if default else "y/N"), "").lower()
        return default if not a else a.startswith(("y", "j"))


def _raw_config(home: Path) -> dict:
    from . import coordinator
    return coordinator.load_json(home / "config.json", {}) or {}


def _save_config(home: Path, cfg: dict) -> None:
    from . import coordinator
    coordinator.save_json(home / "config.json", cfg)


def find_llama(port_hint: int = 1234):
    """A running llama-server: (pid, spec, port). Prefers the one on port_hint; else the only one running."""
    try:
        import psutil
    except Exception:
        return None
    from . import llama_restart
    procs = [p for p in psutil.process_iter(["pid", "name"]) if "llama-server" in (p.info.get("name") or "").lower()]
    found = []
    for p in procs:
        spec = llama_restart._process_spec(p.info["pid"])
        if not spec:
            continue
        argv = spec["argv"]
        port = 8080  # llama-server's own default
        for i, a in enumerate(argv[:-1]):
            if a == "--port":
                try:
                    port = int(argv[i + 1])
                except ValueError:
                    pass
        found.append((p.info["pid"], spec, port))
    for f in found:
        if f[2] == port_hint:
            return f
    return found[0] if len(found) == 1 else None


def write_start_script(home: Path, spec: dict) -> Path:
    """A start script that reproduces the running server: same folder, same command line."""
    cwd = spec.get("cwd") or str(Path(spec["argv"][0]).parent)
    if os.name == "nt":
        path = home / "start-llama.bat"
        body = "@echo off\r\nrem Written by `hawk setup` from the running llama-server's command line.\r\n" \
               'cd /d "%s"\r\n%s\r\n' % (cwd, subprocess.list2cmdline(spec["argv"]))
        path.write_text(body, encoding="utf-8")
    else:
        path = home / "start-llama.sh"
        body = "#!/bin/sh\n# Written by `hawk setup` from the running llama-server's command line.\ncd %s\nexec %s\n" % (
            shlex.quote(cwd), shlex.join(spec["argv"]))
        path.write_text(body, encoding="utf-8")
        path.chmod(0o755)
    return path


def run(assume_yes=False, answers=None, install_tasks=True, start=True) -> int:
    from . import coordinator
    from .cli import PACKAGE_DIR
    ask = Asker(assume_yes, answers)
    home = coordinator.home_dir()
    home.mkdir(parents=True, exist_ok=True)
    cfg_path = home / "config.json"
    _say("Hawk setup  (Enter accepts the [default])")
    _say("config: %s" % cfg_path)
    if not cfg_path.exists():
        shutil.copyfile(PACKAGE_DIR / "config.example.json", cfg_path)
    cfg = _raw_config(home)

    # 1. ntfy
    topic = coordinator.ensure_ntfy_topic(cfg, persist=False) or cfg["notify"]["ntfy"]["topic"]
    server = (cfg.get("notify", {}).get("ntfy", {}).get("server") or "https://ntfy.sh").rstrip("/")
    _save_config(home, cfg)
    _say("\n1. Phone alerts (ntfy)")
    _say("   Your private topic: %s  (treat it like a password)" % topic)
    _say("   Subscribe in the ntfy app, or open %s/%s" % (server, topic))

    # 2. llama-server
    _say("\n2. llama-server")
    port_hint = int(cfg.get("llama_api_port") or 1234)
    live = find_llama(port_hint)
    if live:
        pid, spec, port = live
        _say("   running: pid %s on port %s (%s)" % (pid, port, Path(spec["argv"][0]).name))
        cfg["llama_api_port"] = port
    else:
        _say("   no running llama-server found")
    bat = str(cfg.get("llama_bat_path") or "").strip()
    if bat and Path(bat).exists():
        _say("   start script: %s (kept)" % bat)
    elif live and ask.yes("   Write a start script from the running server's command line?", True):
        bat = str(write_start_script(home, live[1]))
        _say("   start script written: %s" % bat)
    else:
        while True:
            bat = ask.text("   Path to the script that starts your llama-server (Enter = skip)", "")
            if not bat or Path(bat).exists():
                break
            _say("   not found: %s" % bat)
            if assume_yes or answers is not None and not ask.answers:
                bat = ""
                break
        if not bat:
            _say("   skipped: Hawk cannot start a dead llama-server until llama_bat_path is set (rerun hawk setup)")
    cfg["llama_bat_path"] = bat
    if not live:
        port = ask.text("   llama-server port", str(port_hint))
        cfg["llama_api_port"] = int(port) if str(port).isdigit() else port_hint

    # 3. project folder
    _say("\n3. Project")
    current = str(cfg.get("project_dir") or "")
    current = current if current and Path(current).is_dir() else os.getcwd()  # the example's placeholder is no folder
    cfg["project_dir"] = ask.text("   Folder your agent works in", current)
    _save_config(home, cfg)

    # 4. failsafe + plugin
    _say("\n4. Failsafe and plugin")
    from . import cli
    if install_tasks and ask.yes("   Install the watchdog (keeps Hawk and llama-server running)?", True):
        cli.cmd_install_watchdog(type("A", (), {"remove": False, "every": 2})())
    if install_tasks and ask.yes("   Install the OpenCode llama-restart plugin (restarts only if you enable it)?", False):
        cli.cmd_install_plugin(type("A", (), {"remove": False, "dir": None})())

    # 5. test + start
    _say("\n5. Start")
    if ask.yes("   Send a test message to your phone?", True):
        from . import notify
        _say("   sent" if notify.send_test(coordinator.load_config()) else "   could not send (check the network)")
    if start and ask.yes("   Start Hawk now?", True):
        cli.cmd_ensure(type("A", (), {"port": 8765})())
        _say("   dashboard: http://127.0.0.1:8765")
    for p in coordinator.validate_config(coordinator.load_config()):
        _say("   note: %s" % p)
    _say("\nDone. Change anything later with `hawk setup` again or in the dashboard's settings.")
    return 0
