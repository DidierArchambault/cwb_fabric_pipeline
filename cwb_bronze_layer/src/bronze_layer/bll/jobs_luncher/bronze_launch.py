import logging
import os

from bronze_layer.bll.jobs_def.clean_cols_names import clean_col_names
from bronze_layer.bll.jobs_def.export_data import export_to_delta
from bronze_layer.bll.jobs_def.add_technicals import add_technicals
from bronze_layer.bll.jobs_def.remap_cols_names import rename_cols
from bronze_layer.bll.helpers.guard import _guard
from bronze_layer.bll.jobs_def.ingest_data import ingest_data
from bronze_layer.dal.helpers.path_resolver import resolve_path
from bronze_layer.cli.cli import RunContext
from bronze_layer.resources.config_bz.configuration import Configuration

logger = logging.getLogger(__name__)


def bronze_launch(ctx: RunContext, config: Configuration, pipeline_run_id: str) -> bool:
    """
    Launch the bronze layer job to merge parquet files.

    Args:
        parquets_path (str): The path to the directory containing parquet files.
    """
    if ctx.config_source == "local":
        logger.info("Launching bronze layer in local mode.")
        parquets_path_landing = resolve_path(config, ctx, "landing")
        parquets_path_out = resolve_path(config, ctx, "_temp_sink")
        os.makedirs(parquets_path_out, exist_ok=True)
        table_name = parquets_path_landing.parts[-1]
    elif ctx.config_source == "fabric":
        logger.info("Launching bronze layer in fabric mode.")
        parquets_path_landing = resolve_path(config, ctx, "landing")
        parquets_path_out = resolve_path(config, ctx, "bronze_db")
        table_name = parquets_path_landing.parts[-1]
    else:
        raise ValueError(f"Unknown config source: {ctx.config_source}")

    if ctx.work == "all":
        logger.info("Running all steps for bronze layer.")
        try:
            df_raw = _guard(
                "bronze_ingest",
                lambda: ingest_data(ctx, config, str(parquets_path_landing)),
            )
            df_cleaned = _guard(
                "bronze_cleansing", lambda: clean_col_names(config, df_raw, ctx)
            )
            df_renamed = _guard(
                "bronze_cleansing", lambda: rename_cols(config, df_cleaned, ctx)
            )
            df_technicals = _guard(
                "bronze_add_technicals",
                lambda: add_technicals(ctx, config, df_renamed, pipeline_run_id),
            )
            _guard(
                "bronze_export",
                lambda: export_to_delta(
                    ctx, config, df_technicals, str(parquets_path_out)
                ),
            )

            return True

        except Exception as e:
            logger.error(f"Error during bronze layer job: {e}")
            raise e

    return False
