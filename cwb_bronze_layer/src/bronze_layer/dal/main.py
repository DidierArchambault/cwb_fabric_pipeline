import sys
import logging
import os
import uuid
import toml
import time

from dotenv import load_dotenv
from importlib import resources

from bronze_layer.cli.cli import RunContext, build_parser
from bronze_layer.dal.dispatch import dispatch
from bronze_layer.dal.helpers.log_writer_init import log_writer
from bronze_layer.dal.helpers.toml_fabric_loader import load_toml_via_spark
from bronze_layer.bll.helpers.spark_session import build_spark_session
from bronze_layer.resources.config_bz.configuration import Configuration, from_dict
from bronze_layer.dal.helpers.path_resolver import resolve_under_root

# TODO: unit test pour le cleanisng_utils
# TODO: add a mecanism to handle shema change in bronze (non critical and critical)
# TODO: use log4jLogger for spark operations instead of python logger (Python's logging module doesn't propagate to executor logs)
# TODO: track overall ppipeline metrics (duration, number of rows processed, etc.) and log them for runs_logs


def _bootstrap_logger() -> logging.Logger:
    """Minimal logger used only to stdout logs for config load"""
    logger = logging.getLogger("bootstrap")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        logger.addHandler(h)
    logger.propagate = False
    return logger


def main(argv: list[str] | None = None) -> int:
    """
    Main entry point for the bronze Spark Job.\n
    Sets up logging, parses command-line arguments, loads configuration,
    and dispatches the command to the appropriate handler.\n
    Can be called with a custom list of arguments for testing purposes. If None, uses sys.argv.
    For pipeline runs, the expected args are: --env, --config, --xcenter, --work.\n
    :param argv: List of command-line arguments. If None, uses sys.argv.
    :return: Exit code (0 for success).
    """
    ts_in = time.strftime("%Y%m%d-%H%M%S")  # timestamp IN for the run
    # bootstrap = _bootstrap_logger() # ONLY for preliminary logs before config load, replaced by log_writer after config is loaded
    load_dotenv()

    # Build parser and parse arguments
    if argv is None:
        argv = sys.argv[1:]
    parser = build_parser()

    if not argv:
        parser.print_help()
        return 0

    args = parser.parse_args(argv)
    # env = getattr(args, "env", "dev")
    config_source = args.config_source
    config_path = args.config_path
    print(f"this the source of the config: {config_source}")
    print(f"this the path of the config: {config_path}")
    print("ARGV=", argv)
    print("PARSED=", vars(args))
    config_dict = None

    # build RunContext used to pass the args down the line
    ctx = RunContext(
        env=getattr(args, "env", None),
        layer=getattr(args, "layer", None),
        work=getattr(args, "work", None),
        cmd=getattr(args, "cmd", None),
        xcenter=getattr(args, "xcenter", None),
        config_source=getattr(args, "config_source", None),
        config_path=getattr(args, "config_path", None),
    )

    # Build configuration from .toml (local or fabric) and create config object
    try:
        if config_source == "fabric":
            print("config_source=", config_source)
            print("BRONZE_CONFIG_PATH=", config_path)
            if not config_path:
                raise ValueError("config=fabric, but BRONZE_CONFIG_PATH is missing.")
            config_dict = load_toml_via_spark(
                build_spark_session(ctx, "BronzeLayerSession", 1000), config_path
            )

        elif config_source == "local":
            with resources.files("bronze_layer.resources").joinpath(
                f"bz_config_{config_source}.toml"
            ).open("r", encoding="utf-8") as f:
                config_dict = toml.load(f)

        else:
            raise ValueError(
                f"Invalid config_source: {config_source} (expected: local|fabric)"
            )

        config: Configuration = from_dict(config_dict)

        if config_source == "local":
            print("Configuration loaded from local resources.")
            runtime_root = os.getenv("BRONZE_RUNTIME_ROOT")
            if not runtime_root:
                raise ValueError(
                    "BRONZE_RUNTIME_ROOT missing (needed only for config_source=local)."
                )

            config.storage_configuration.landing_base = resolve_under_root(
                runtime_root, config.storage_configuration.landing_base
            )
            config.storage_configuration.bronze_db = resolve_under_root(
                runtime_root, config.storage_configuration.bronze_db
            )
            config.storage_configuration.bronze_files = resolve_under_root(
                runtime_root, config.storage_configuration.bronze_files
            )

            config.observability_configuration.runs_logs = resolve_under_root(
                runtime_root, config.observability_configuration.runs_logs
            )
            config.observability_configuration.events_logs = resolve_under_root(
                runtime_root, config.observability_configuration.events_logs
            )
            config.observability_configuration.artifacts_root = resolve_under_root(
                runtime_root, config.observability_configuration.artifacts_root
            )

            config.rules_configuration.cleaning_json = resolve_under_root(
                runtime_root, config.rules_configuration.cleaning_json
            )
            config.rules_configuration.rename_map_json = resolve_under_root(
                runtime_root, config.rules_configuration.rename_map_json
            )
            config.rules_configuration.technicals_json = resolve_under_root(
                runtime_root, config.rules_configuration.technicals_json
            )

    except Exception as e:
        _bootstrap_logger().exception("Error loading configuration: %s", e)
        return 1

    # Set up logging
    pipeline_run_id = str(uuid.uuid1())  # unique id based on timestep and user_ID
    logger = log_writer(ctx=ctx, config=config, pipeline_run_id=pipeline_run_id)
    logger.info("Starting pipeline run for bronze layer, run_id=%s", pipeline_run_id)

    # dispatch command to appropriate handler
    print()
    dispatch(ctx=ctx, config=config, pipeline_run_id=pipeline_run_id)

    ts_out = time.strftime("%Y%m%d-%H%M%S")  # timestamp OUT for the run
    # TODO: track overall ppipeline metrics (duration, number of rows processed, etc.) and log them for runs_logs
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
