import logging
import sys
from pathlib import Path
from bll.helpers.path_maker_temp import build_outlogs_path


logger = logging.getLogger(__name__)


def log_writer(ctx, path: str, pipeline_run_id) -> None:
    log_path = build_outlogs_path(ctx.config)

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
    logger.info("Starting pipeline run for dataset=%s run_id=%s", ctx.step, pipeline_run_id)