import os
import sys
import uuid
import logging
import toml

from pathlib import Path

from dal.cli.cli import build_parser
from dal.dispatch import dispatch
import configuration.configuration


# TODO: add more outputs to log_file
# TODO: make sure you understand how to use the MS Fabric interaction
# TODO: review CLI

# TODO: add log_writer in the main, and make sure it works as expected (log file creation, log format, etc.)

def main(argv: list[str] | None = None) -> int:
    """Main entry point for the data pipeline application.\n
    Sets up logging, parses command-line arguments, loads configuration,
    and dispatches the command to the appropriate handler.\n
    :param argv: List of command-line arguments. If None, uses sys.argv.
    :return: Exit code (0 for success)."""
    if argv is None:
        argv = sys.argv[1:]

    parser = build_parser()

    if not argv:
        parser.print_help()
        return 0

    args = parser.parse_args(argv)

    pipeline_run_id = str(uuid.uuid4())

    log_dir = Path(__file__).parent / "run_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{args.layer}_pipeline.log"

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
    # ALLER CHERCHER LE LOG_WRITER.PY

    logger = logging.getLogger(__name__)
    logger.info("Starting pipeline run for dataset=%s run_id=%s", args.layer, pipeline_run_id)

    dispatch(args, config=configuration.configuration.from_dict(
        toml.load(
            os.path.join(Path(__file__).parent.parent, "bll", "ressources", "global_config.toml")
        )
    ))
    return 0

if __name__ == "__main__":
    main()
