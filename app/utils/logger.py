"""Logging utility using Python's standard logging module."""
import logging
import sys
from typing import Optional

# Configure logger
_logger: Optional[logging.Logger] = None


def get_logger(name: str = "etl_pipeline") -> logging.Logger:
    """Get or create the logger instance."""
    global _logger
    if _logger is None:
        _logger = logging.getLogger(name)
        _logger.setLevel(logging.INFO)
        
        # Create console handler if not exists
        if not _logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            _logger.addHandler(handler)
    
    return _logger


def log(message: str, level: str = "INFO") -> None:
    """Log a message with the specified level."""
    logger = get_logger()
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.log(log_level, message)


def info(message: str) -> None:
    """Log an info message."""
    log(message, "INFO")


def warning(message: str) -> None:
    """Log a warning message."""
    log(message, "WARNING")


def error(message: str) -> None:
    """Log an error message."""
    log(message, "ERROR")


def debug(message: str) -> None:
    """Log a debug message."""
    log(message, "DEBUG")

