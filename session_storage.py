"""Pure path and cleanup helpers for account and temporary QR sessions."""

import re
import shutil
import time
from pathlib import Path
from typing import Optional

QR_TEMP_MAX_AGE_SECONDS = 24 * 60 * 60
QR_TEMP_DIR_PATTERN = re.compile(r"_qr_temp_\d+_\d+\Z")


def account_directory(accounts_dir: Path, normalized_phone: str) -> Path:
    return Path(accounts_dir) / normalized_phone


def account_session_base_path(accounts_dir: Path, normalized_phone: str) -> Path:
    return account_directory(accounts_dir, normalized_phone) / "session"


def cleanup_stale_qr_dirs(accounts_dir: Path, now: Optional[float] = None) -> int:
    """Remove expired QR temp folders without touching account session folders."""
    accounts_dir = Path(accounts_dir)
    if not accounts_dir.exists():
        return 0

    current_time = time.time() if now is None else now
    removed = 0
    for temp_dir in accounts_dir.iterdir():
        if not temp_dir.is_dir() or not QR_TEMP_DIR_PATTERN.fullmatch(temp_dir.name):
            continue
        try:
            age = current_time - temp_dir.stat().st_mtime
            if age < QR_TEMP_MAX_AGE_SECONDS:
                continue
            shutil.rmtree(temp_dir)
        except OSError:
            continue
        removed += 1
    return removed
