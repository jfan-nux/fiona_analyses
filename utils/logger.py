"""
Centralized logging configuration for the project.

This module provides standardized logging setup that can be reused
across the codebase, ensuring consistent log formatting and behavior.
"""

import logging
import os
import sys
from pathlib import Path
from typing import Optional, Union, Dict, Any

# Default log format includes timestamp, logger name, level and message
DEFAULT_LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Default log level
DEFAULT_LOG_LEVEL = logging.INFO


def setup_logger(
    name: str,
    level: Optional[int] = None,
    format_str: Optional[str] = None,
    log_file: Optional[str] = None,
    log_to_console: bool = True,
) -> logging.Logger:
    """
    Configure and return a logger with the specified settings.

    Args:
        name: Name of the logger
        level: Logging level (default: INFO)
        format_str: Log message format (default: timestamp - name - level - message)
        log_file: Path to log file if file logging is desired
        log_to_console: Whether to log to console (default: True)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Use default level if none specified
    if level is None:
        level = DEFAULT_LOG_LEVEL

    logger.setLevel(level)

    # Use default format if none specified
    if format_str is None:
        format_str = DEFAULT_LOG_FORMAT

    formatter = logging.Formatter(format_str)

    # Clear any existing handlers to avoid duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Add console handler if requested
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # Add file handler if a log file is specified
    if log_file:
        # Ensure the directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# Create a default logger for direct imports
default_logger = setup_logger(__name__)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name, using the default configuration.

    This is a convenience function for quickly getting a logger without
    needing to specify all configuration options.

    Args:
        name: Name of the logger

    Returns:
        Configured logger instance
    """
    return setup_logger(name)