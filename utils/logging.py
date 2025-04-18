import os
import sys
from pathlib import Path
from loguru import logger

# Create logs directory if it doesn't exist
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Remove default logger
logger.remove()

# Define severity levels and their corresponding files
SEVERITY_FILES = {
    "ERROR": "error.log",
    "WARNING": "warning.log",
    "CRITICAL": "critical.log",
    "INFO": "info.log",
    "DEBUG": "debug.log",
    "TRACE": "trace.log",
    "SUCCESS": "success.log"
}

# Common log format for files
FILE_FORMAT = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"

# Common log format for console (with colors)
CONSOLE_FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

# Add console loggers for different severities
logger.add(
    sys.stdout,
    format=CONSOLE_FORMAT,
    level="INFO",
    colorize=True,
    filter=lambda record: record["level"].no < logger.level("WARNING").no
)

logger.add(
    sys.stderr,
    format=CONSOLE_FORMAT,
    level="WARNING",
    colorize=True,
    filter=lambda record: record["level"].no >= logger.level("WARNING").no
)

def configure_file_logging(write_to_files: bool = True):
    """Configure file-based logging based on settings."""
    if write_to_files:
        # Add file loggers for each severity level
        for level, filename in SEVERITY_FILES.items():
            logger.add(
                LOGS_DIR / filename,
                rotation="100 MB",
                retention="7 days",
                compression="zip",
                format=FILE_FORMAT,
                level=level,
                backtrace=True,
                diagnose=True,
                filter=lambda record, level=level: record["level"].name == level
            )

# Export the configured logger
__all__ = ["logger", "configure_file_logging"]
