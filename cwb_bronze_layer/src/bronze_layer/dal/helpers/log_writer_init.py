import logging
import os
import sys
from pathlib import Path
import time


class MaxLevelFilter(logging.Filter):
    def __init__(self, max_level):
        super().__init__()
        self.max_level = max_level  # retrains our handler to a max log level

    def filter(self, record):
        return record.levelno <= self.max_level


def log_writer(ctx, config, pipeline_run_id: str) -> logging.Logger:
    """
    Configure the logging for the application.
    Args:
        ctx: The context of the application, containing layer and work information.
        config: The configuration object containing logging settings.
        pipeline_run_id: A unique identifier for the current pipeline run.
    Returns:
        A configured logger instance.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Clean existing handlers (like the bootstrap logger handler) to avoid duplicate logs
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)

    file_name = f"{ctx.work}_" f"{time.strftime('%Y%m%d-%H%M%S')}_" f"{pipeline_run_id}"

    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    formatter = logging.Formatter(fmt)

    # STDOUT handler (INFO + WARNING)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.addFilter(MaxLevelFilter(logging.WARNING))
    stdout_handler.setFormatter(formatter)

    # STDERR handler (ERROR + CRITICAL)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.ERROR)
    stderr_handler.setFormatter(formatter)

    if ctx.config_source == "local":
        print(f"Logging to console only (local run).")
        log_path = Path(
            config.observability_configuration.events_logs, f"{file_name}.log"
        )
        os.makedirs(log_path.parent, exist_ok=True)
        file_handler = logging.FileHandler(log_path, mode="a")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    else:
        print(
            f"Logging to console (stdout for INFO/WARNING, stderr for ERROR/CRITICAL)."
        )

    root_logger.addHandler(stdout_handler)
    root_logger.addHandler(stderr_handler)

    return logging.getLogger(__name__)
