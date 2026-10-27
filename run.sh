#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

if [[ ! -x .venv/bin/python ]]; then
    echo "Virtual environment not found. Run ./install.sh first." >&2
    exit 1
fi

if [[ ! -f config.json ]]; then
    echo "config.json not found. Run ./install.sh and fill in the generated config.json." >&2
    exit 1
fi

exec .venv/bin/python bot.py
