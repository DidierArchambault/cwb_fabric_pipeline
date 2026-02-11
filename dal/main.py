import os
import sys
import uuid
import toml

from pathlib import Path

from dal.cli.cli import build_parser
from dal.dispatch import dispatch
from dal.helpers.log_writer import log_writer
import configuration.configuration

# TODO: add more outputs to log_file ---DONE---
# TODO: make sure I understand how to use the MS Fabric interaction ---DONE---
# TODO: review CLI ---DONE---
# TODO: dispatch.py add _guard ---DONE---
# TODO: add mapping for column names ---DONE---
# TODO: add cols_cleanup.py ---DONE---
# TODO: change the toml and methode for technicals (use JSON instead)
# TODO: add control table creation
# TODO: add a comparator for the control table to check if the new data is consistent with the previous one (for example, check if the number of rows is not drastically different, or if the distribution of values in certain columns is not drastically different) ---DONE---
# TODO: add hash cols selector from config_file ---DONE---
# TODO: unit test pour le cleanisng_utils
# TODO: add a mecanism to handle shema change in bronze (non critical and critical)
# TODO: add log_writer in the main, and make sure it works as expected (log file creation, log format, etc.) ---DONE---
# TODO: use log4jLogger for spark operations instead of python logger (Python's logging module doesn't propagate to executor logs)


def main(argv: list[str] | None = None) -> int:
    """
    Main entry point for the data pipeline application.\n
    Sets up logging, parses command-line arguments, loads configuration,
    and dispatches the command to the appropriate handler.\n
    :param argv: List of command-line arguments. If None, uses sys.argv.
    :return: Exit code (0 for success).
    """
    # Build parser and parse arguments
    if argv is None:
        argv = sys.argv[1:]
    parser = build_parser()

    if not argv:
        parser.print_help()
        return 0

    args = parser.parse_args(argv)
    ENV = getattr(args, "env", "dev")

    # Build configuration from global_config.toml
    config = configuration.configuration.from_dict(
        toml.load(
            os.path.join(
                Path(__file__).parent.parent,
                "bll",
                "ressources",
                f"global_config_{ENV}.toml",
            )
        )
    )
    print(f"Configuration loaded for environment: {ENV}")

    # Set up logging
    pipeline_run_id = str(uuid.uuid4())  # unique id based on timestep and user_ID
    logger = log_writer(ctx=args, config=config, pipeline_run_id=pipeline_run_id)
    logger.info(
        "Starting pipeline run for dataset=%s run_id=%s", args.layer, pipeline_run_id
    )

    # dispatch command to appropriate handler
    dispatch(args, config=config, pipeline_run_id=pipeline_run_id)
    return 0


if __name__ == "__main__":
    main()
