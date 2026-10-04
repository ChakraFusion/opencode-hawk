#!/usr/bin/env sh
# Hawk quickstart (Linux): starts the monitor and the dashboard in the
# background. Logs: monitor.out / dashboard.out. Stop with ./quick_kill.sh.
set -eu
cd "$(dirname "$0")"

PY=python3
command -v "$PY" >/dev/null 2>&1 || PY=python
if ! command -v "$PY" >/dev/null 2>&1; then
    echo "[error] Python 3 not found. Install it and re-run." >&2
    exit 1
fi
echo "Using: $("$PY" --version)"

# first run: create config.json from the template
if [ ! -f config.json ]; then
    "$PY" -c "import psutil" >/dev/null 2>&1 || "$PY" -m pip install --user -r requirements.txt
    "$PY" -m opencode_hawk init
    echo "        Edit config.json (at least project_dir), then re-run."
    exit 0
fi

# dependency check
"$PY" -c "import psutil" >/dev/null 2>&1 || "$PY" -m pip install --user -r requirements.txt

echo "[1/2] monitor   : $PY -m opencode_hawk monitor --interval 3"
nohup "$PY" -m opencode_hawk monitor --interval 3 >monitor.out 2>&1 &
echo "[2/2] dashboard : $PY -m opencode_hawk dashboard --port 8765 --open"
nohup "$PY" -m opencode_hawk dashboard --port 8765 --open >dashboard.out 2>&1 &

echo
echo "Hawk is running. Dashboard: http://127.0.0.1:8765"
echo "Stop it with ./quick_kill.sh"
