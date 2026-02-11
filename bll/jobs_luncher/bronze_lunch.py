import logging

from bll.jobs_def.clean_cols_names import clean_col_names
from bll.jobs_def.export_data import export_to_delta
from bll.jobs_def.add_technicals import add_technicals
from bll.jobs_def.remap_cols_names import rename_cols
from bll.helpers.guard import _guard
from bll.jobs_def.ingest_data import ingest_data
from dal.helpers.path_resolver import resolve_path
from dal.cli.cli import RunContext
from configuration.configuration import Configuration

logger = logging.getLogger(__name__)


def bronze_lunch(ctx: RunContext, config: Configuration, pipeline_run_id: str) -> None:
    """
    Launch the bronze layer job to merge parquet files.

    Args:
        parquets_path (str): The path to the directory containing parquet files.
    """
    parquets_path_landing = resolve_path(config, ctx, "landing")
    parquets_path_deltalake = resolve_path(config, ctx, "deltalake")
    table_name = parquets_path_landing.parts[-1]

    if ctx.work == "all":
        logger.info("Running all steps for bronze layer.")
        try:
            df_raw = _guard(
                "bronze_ingest",
                lambda: ingest_data(ctx, config, str(parquets_path_landing)),
            )
            df_cleaned = _guard(
                "bronze_cleansing", lambda: clean_col_names(config, df_raw)
            )
            df_renamed = _guard(
                "bronze_cleansing", lambda: rename_cols(config, df_cleaned)
            )
            df_technicals = _guard(
                "bronze_add_technicals",
                lambda: add_technicals(ctx, config, df_renamed, pipeline_run_id),
            )
            _guard(
                "bronze_export",
                lambda: export_to_delta(
                    ctx, config, df_technicals, str(parquets_path_deltalake)
                ),
            )

        except Exception as e:
            print(f"Error during bronze layer job: {e}")
            raise e

    return None
