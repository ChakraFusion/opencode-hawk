"""Pytest entry point for Hawk's stdlib self-tests.

The four ``selftest_*.py`` scripts stay the single source of truth (they are
hermetic and runnable with plain ``python``).  This wrapper imports each one
in *process* and calls its entry point, so

    python -m pytest -q

runs the same scenarios while coverage can see the ``opencode_hawk`` package
(in-process, no subprocess to instrument).  The scripts are run in separate
pytest cases so a failure names the suite.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parent

# (param id, script file, callable that returns an exit code)
SUITES = [
    ("coordinator", "selftest_coordinator.py", "self_test"),
    ("dashboard", "selftest_dashboard.py", "self_test"),
    ("notify", "selftest_notify.py", "main"),
    ("linux", "selftest_linux.py", "main"),
]


def _load_suite(modname: str, filename: str):
    """Import a selftest script as a module, exactly as ``python <file>``."""
    spec = importlib.util.spec_from_file_location(modname, TESTS_DIR / filename)
    assert spec and spec.loader, filename
    module = importlib.util.module_from_spec(spec)
    sys.modules[modname] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("script,entry", [s[1:] for s in SUITES], ids=[s[0] for s in SUITES])
def test_selftest(script: str, entry: str):
    # Unique module name so nothing is shadowed across parametrizations.
    module = _load_suite("hawk_selftest_" + script[:-3], script)
    assert getattr(module, entry)() == 0
