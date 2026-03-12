from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging() -> logging.Logger:
    """
    Egységes logger a projekthez.

    - konzol log
    - file log
    - rotáló log file (10MB)
    """

    log_dir = Path("data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("shop_sync")

    # ha már inicializálva lett egyszer
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    # konzol log
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # file log
    file_handler = RotatingFileHandler(
        log_dir / "shop_sync.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info("Logger initialized")

    return logger