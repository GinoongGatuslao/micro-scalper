from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from threading import Lock


_RATE_LIMIT_SECONDS = 5.0
_last_emitted: dict[str, float] = {}
_lock = Lock()


def say(event: str, **fields) -> None:
    if not _enabled():
        return
    message = _format_message(event, fields)
    now = datetime.now(timezone.utc)
    monotonic_seconds = now.timestamp()
    with _lock:
        last_seen = _last_emitted.get(message)
        if last_seen is not None and monotonic_seconds - last_seen < _RATE_LIMIT_SECONDS:
            return
        _last_emitted[message] = monotonic_seconds
    sys.stdout.write(f"[HUMAN] {now.isoformat()} {message}\n")
    sys.stdout.flush()


def _enabled() -> bool:
    raw = os.getenv("HUMAN_CONSOLE", "true")
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _format_message(event: str, fields: dict[str, object]) -> str:
    if fields:
        try:
            return event.format(**fields)
        except Exception:
            extras = " ".join(f"{key}={value}" for key, value in sorted(fields.items()))
            return f"{event} {extras}".strip()
    return event


def _reset_rate_limit_state() -> None:
    with _lock:
        _last_emitted.clear()
