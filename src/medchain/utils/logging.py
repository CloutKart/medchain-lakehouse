"""Structured logging that reads the same locally and in Databricks driver logs."""

from __future__ import annotations

import logging
import os
import sys

_CONFIGURED = False

FORMAT = "%(asctime)s %(levelname)-7s [%(name)s] %(message)s"


def setup_logging(level: str | None = None) -> None:
    """Configure root logging once. Safe to call from every entry point."""
    global _CONFIGURED
    if _CONFIGURED:
        return
    resolved = (level or os.environ.get("MEDCHAIN_LOG_LEVEL", "INFO")).upper()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(resolved)
    # py4j is extremely chatty at INFO and drowns out our own messages.
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    setup_logging()
    return logging.getLogger(name)


def banner(log: logging.Logger, title: str, **context: object) -> None:
    """Log a visually distinct step header with its context.

    Pipeline logs get read during incidents, usually in a hurry. Making each step
    boundary obvious is worth the four extra lines.
    """
    log.info("=" * 72)
    log.info(title)
    for key, value in context.items():
        log.info("  %-16s %s", key, value)
    log.info("=" * 72)
