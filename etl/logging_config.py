import logging

from pythonjsonlogger import jsonlogger


def setup_logging(log_level: str = "INFO", log_format: str = "text") -> None:
    """Configures the global logger with a specific level and format."""
    logger = logging.getLogger()
    logger.setLevel(log_level.upper())

    handler = logging.StreamHandler()

    formatter: logging.Formatter
    if log_format.lower() == "json":
        formatter = jsonlogger.JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s")  # type: ignore[attr-defined]
    else:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    handler.setFormatter(formatter)

    # Avoid adding handlers multiple times (useful for interactive sessions/tests)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(handler)
