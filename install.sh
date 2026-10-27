#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

python3 - <<'PY'
import sys

if sys.version_info < (3, 9):
    raise SystemExit("Python 3.9 or newer is required.")
PY

if [[ ! -d .venv ]]; then
    python3 -m venv .venv
fi

.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt

if [[ ! -f config.json ]]; then
    cp config.example.json config.json
    echo "Created config.json from config.example.json. Fill in your Telegram credentials before starting the bot."
else
    echo "Keeping existing config.json."
fi
chmod 600 config.json

echo "Installation complete. Edit config.json, then run ./run.sh."
