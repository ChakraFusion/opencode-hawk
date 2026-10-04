#!/usr/bin/env sh
# Hawk quick kill (Linux): stops the monitor and dashboard started by
# quickstart.sh, matched by script name in the command line.
cd "$(dirname "$0")"
found=0
for pid in $(pgrep -f "python[0-9.]* .*(coordinator|dashboard)\.py" || true); do
    echo "killing PID $pid: $(tr '\0' ' ' </proc/"$pid"/cmdline 2>/dev/null)"
    kill "$pid" 2>/dev/null && found=$((found + 1))
done
if [ "$found" -eq 0 ]; then
    echo "no coordinator.py / dashboard.py process running."
else
    echo "stopped $found process(es)."
fi
