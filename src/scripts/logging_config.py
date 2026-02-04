"""Shared logging configuration for benchmark scripts."""

import logging


def setup_logger(name: str) -> logging.Logger:
    """Configure and return a logger with standard formatting."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    return logging.getLogger(name)
