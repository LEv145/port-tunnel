import logging
import sys

import colorlog


def setup_logging(level: int = logging.INFO) -> None:
    root_logger = logging.getLogger()

    if getattr(root_logger, "_color_logging_configured", False):
        return

    root_logger.setLevel(level)

    handler = colorlog.StreamHandler(stream=sys.stdout)
    handler.setLevel(level)

    formatter = colorlog.ColoredFormatter(
        fmt=(
            "%(log_color)s"
            "%(asctime)s | "
            "%(levelname)-8s | "
            "%(name)s | "
            "%(message)s"
            "%(reset)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
        reset=True,
    )

    handler.setFormatter(formatter)

    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    root_logger._color_logging_configured = True
