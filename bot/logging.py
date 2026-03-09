from __future__ import annotations

import logging as pylogging


def configure_logging(level: str = "INFO") -> pylogging.Logger:
    pylogging.basicConfig(
        level=getattr(pylogging, level.upper(), pylogging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return pylogging.getLogger("bot")
