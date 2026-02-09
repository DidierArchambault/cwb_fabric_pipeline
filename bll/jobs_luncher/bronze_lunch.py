import os
import logging

from bll.jobs_def.export_data import export_to_delta
from bll.jobs_def.add_technicals import add_technicals
from configuration.configuration import Configuration
from dal.cli.cli import RunContext
from bll.helpers.guard import _guard
from bll.jobs_def.ingest_data import ingest_data
from bll.helpers.path_maker_temp import build_landing_path, build_deltalake_path

logger = logging.getLogger(__name__)


def bronze_lunch(ctx: RunContext, config: Configuration) -> None:
    """
    Launch the bronze layer job to merge parquet files.

    Args:
        parquets_path (str): The path to the directory containing parquet files.
    """
    parquets_path_landing = build_landing_path(config)
    parquets_path_deltalake = build_deltalake_path(config)
    os.makedirs(parquets_path_deltalake, exist_ok=True) # TODO:delete for prod

    if ctx.work == "all":
        logger.info("Running all steps for bronze layer.")
        try:
            df_raw = _guard("bronze_ingest", lambda: ingest_data(ctx, config, str(parquets_path_landing)))
            df_technicals = _guard("bronze_add_technicals", lambda: add_technicals(ctx, config, df_raw))
            _guard("bronze_export", lambda: export_to_delta(ctx, config, df_technicals, str(parquets_path_deltalake)))

        except Exception as e:
            print(f"Error during bronze layer job: {e}")
            raise e

    return None