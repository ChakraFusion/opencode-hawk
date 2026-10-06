#!/bin/sh
# Hawk installer for Linux (and other Unix-like systems).
#   curl -fsSL https://raw.githubusercontent.com/ChakraFusion/opencode-hawk/main/install.sh | sh
# Installs pipx if needed, installs (or upgrades) Hawk with it, then runs the guided `hawk setup`.
# Environment: HAWK_SOURCE=<path or URL>  HAWK_YES=1 (setup takes every default)  HAWK_SKIP_SETUP=1
set -eu
SOURCE="${HAWK_SOURCE:-https://github.com/ChakraFusion/opencode-hawk/archive/refs/heads/main.zip}"

PY=""
for c in python3 python; do
    if command -v "$c" >/dev/null 2>&1 && "$c" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)'; then
        PY="$c"; break
    fi
done
if [ -z "$PY" ]; then
    echo "Hawk needs Python 3.10 or newer; install it with your package manager, then run this again." >&2
    exit 1
fi

if ! "$PY" -m pipx --version >/dev/null 2>&1; then
    echo "Installing pipx ..."
    "$PY" -m pip install --user --quiet pipx || {
        echo "pip could not install pipx; install it with your package manager (e.g. apt install pipx)." >&2; exit 1; }
    "$PY" -m pipx ensurepath >/dev/null
fi

echo "Installing Hawk from $SOURCE ..."
"$PY" -m pipx install --force "$SOURCE"
BIN="$("$PY" -m pipx environment --value PIPX_BIN_DIR)"
echo "Hawk installed: $BIN/hawk"
[ "${HAWK_SKIP_SETUP:-}" = "1" ] && exit 0
# The setup asks questions: read them from the terminal even when this script came through a pipe.
if [ "${HAWK_YES:-}" = "1" ]; then "$BIN/hawk" setup --yes; else "$BIN/hawk" setup </dev/tty; fi
echo "Open a new terminal to use the hawk command (pipx added $BIN to your PATH)."
