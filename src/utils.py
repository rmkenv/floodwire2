"""
utils.py — Config loading and structured logging setup.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

import yaml


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load and validate config.yaml.  Raises FileNotFoundError / KeyError on bad config."""
    config_path = Path(path) if path else _DEFAULT_CONFIG_PATH
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            "Copy config.example.yaml → config.yaml and fill in your credentials."
        )
    with config_path.open() as fh:
        cfg: dict[str, Any] = yaml.safe_load(fh)

    # Basic validation
    required_keys = [("api", "thenewsapi_token"), ("database", "host")]
    for section, key in required_keys:
        if not cfg.get(section, {}).get(key):
            raise KeyError(f"Missing required config key: {section}.{key}")

    return cfg


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str, cfg: dict[str, Any] | None = None) -> logging.Logger:
    """Return a configured logger. Call once per module at import time."""
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers when re-imported
    if logger.handlers:
        return logger

    level_str: str = "INFO"
    log_file: str | None = None
    if cfg:
        level_str = cfg.get("etl", {}).get("log_level", "INFO")
        log_file = cfg.get("etl", {}).get("log_file")

    level = getattr(logging, level_str.upper(), logging.INFO)
    logger.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # Console
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # Optional file handler
    if log_file:
        try:
            fh = logging.FileHandler(log_file)
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except OSError as exc:
            logger.warning("Could not open log file %s: %s", log_file, exc)

    return logger
