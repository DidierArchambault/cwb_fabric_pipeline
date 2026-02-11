import logging
import sys
import time

from pathlib import Path

# from bll.helpers.path_maker_temp import build_outlogs_path
from configuration.configuration import Configuration

logger = logging.getLogger(__name__)


def log_writer(ctx, config: Configuration, pipeline_run_id: str) -> logging.Logger:
    file_name = (
        f"{ctx.layer}_"
        f"{ctx.work}_"
        f"{time.strftime('%Y%m%d-%H%M%S')}_"
        f"{pipeline_run_id}"
    )
    log_path = Path(config.logging_configuration.main_logging_path, f"{file_name}.log")

    stream_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(log_path, mode="a")

    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    formatter = logging.Formatter(fmt)
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)
    root_logger.addHandler(stream_handler)
    root_logger.addHandler(file_handler)

    logger = logging.getLogger(__name__)

    return logger
