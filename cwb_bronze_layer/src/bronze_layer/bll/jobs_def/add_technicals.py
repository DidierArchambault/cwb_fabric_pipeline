from importlib import resources
from bronze_layer.bll.helpers.spark_session import build_spark_session
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from typing import Any, Dict, List
from urllib.parse import urlparse
from bronze_layer.resources.config_bz.configuration import Configuration
from bronze_layer.cli.cli import RunContext

import logging
import json

from bronze_layer.resources.config_bz.configuration import Configuration

logger = logging.getLogger(__name__)


# ___________ QUESTIONS ___________
# c'est quoi claim_number?
# claim_number existe déjà


# --------------------------------
# --------JSON Loader-------------
# -------Mapping Technicals-------
# --------------------------------
def _read_text_via_spark(spark, path_str: str) -> str:
    df = spark.read.text(path_str)
    lines = [r["value"] for r in df.collect()]
    return "\n".join(lines)


def _load_technicals_list(config: Configuration, ctx: RunContext) -> Dict:
    path_str = config.rules_configuration.technicals_json
    logger.info("Loading technicals JSON from: %s", path_str)

    if ctx.config_source == "local":
        from pathlib import Path

        return json.loads(Path(path_str).read_text(encoding="utf-8"))

    if ctx.config_source == "fabric":
        parsed = urlparse(path_str)
        if parsed.scheme in ("abfs", "abfss"):
            spark = build_spark_session(ctx, "technicals_json_loader", max_to_str=1000)
            return json.loads(_read_text_via_spark(spark, path_str))
        else:
            from pathlib import Path

            return json.loads(Path(path_str).read_text(encoding="utf-8"))

    raise ValueError(f"Unknown config source: {ctx.config_source}")


def _build_technicals_add_map(technicals_to_add_list: Dict[str, Any]) -> List[str]:
    """
    Build a map of technical columns to add based on the cleaning rules.
    Only columns with a True flag will be included.
    """
    technicals_to_add = technicals_to_add_list.get("technicals_to_add", {})
    if not isinstance(technicals_to_add, dict):
        raise TypeError(
            "rules_configuration['technicals_to_add'] must be a dict of {name: bool}"
        )

    enabled_technicals = [
        name for name, flag in technicals_to_add.items() if flag is True
    ]
    return enabled_technicals


def _retrieve_key_cols(
    technicals_to_add_list: Dict[str, Any], key_type: str
) -> List[str]:
    """
    Retrieve the list of columns to use for key generation from the technicals configuration.
    """
    cols_for_key = technicals_to_add_list.get(key_type, {})
    if not isinstance(cols_for_key, list):
        raise TypeError(
            f"rules_configuration['{key_type}'] must be a list of columns to include in {key_type} generation"
        )

    enabled_for_key = [col for col in cols_for_key if isinstance(col, str)]
    return enabled_for_key


# --------------------------------
# ----technical rows fonctions----
# --------------------------------
def _row_hash_id(config: Configuration, ctx: RunContext) -> Column:
    row_hash_id = F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(c).cast("string"), F.lit(""))
                for c in _retrieve_key_cols(
                    _load_technicals_list(config, ctx), "hash_key_used_cols"
                )
            ],
        ),
        256,
    )
    return row_hash_id


def _ingestion_run_id(ctx: RunContext, pipeline_run_id: str) -> Column:
    ingestion_run_id = F.lit(getattr(ctx, "run_id", pipeline_run_id))
    return ingestion_run_id


def _claim_sk(config: Configuration, ctx: RunContext) -> Column:
    sk_pattern = f"wcbns_{ctx.xcenter}_{config.storage_configuration.landing_base.strip()}_"  # to-do: modifier pour aller chercher le vrai nom de table
    claim_sk = F.sha2(
        F.concat_ws("||", F.lit(sk_pattern), F.col("claim_id").cast("string")), 256
    )
    return claim_sk


def _claim_hk(config: Configuration, ctx: RunContext) -> Column:
    hk_cols = _retrieve_key_cols(
        _load_technicals_list(config, ctx), "hash_key_used_cols"
    )
    claim_sk = F.sha2(
        F.concat_ws(
            "||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hk_cols]
        ),
        256,
    )
    return claim_sk


# --------------------------------
# ----orchestrator functions-----
# --------------------------------
def add_technicals(
    ctx: RunContext, config: Configuration, df: DataFrame, pipeline_run_id: str
) -> DataFrame:
    """
    Add technical columns to the DataFrame based on the configuration.\n
    Specify the columns to add in the configuration file, and they will\n
    be computed and added to the DataFrame.\n
    Args:
        ctx: The run context containing metadata about the job execution.
        config: The configuration object containing technical column settings.
        df: The input DataFrame to which technical columns will be added.
        pipeline_run_id: The unique identifier for the current pipeline run."""
    # Load tecnical configuration (contains which technical columns to add)
    # tech_cfg = config.technicals_configuration
    flagged_technicals = _build_technicals_add_map(_load_technicals_list(config, ctx))
    # raw DataFrame
    df_raw = df
    # possible columns to add, flags are in the config file, and if true, the corresponding column will be added to the DataFrame
    flag_to_column = {
        "row_hash_id": "row_hash_id",
        "ingestion_run_id": "ingestion_run_id",
        "processing_ts": "processing_ts",
        "claim_sk": "claim_sk",
        "claim_hk": "claim_hk",
        "claim_number": "claim_number",
        "record_hash": "record_hash",
        "source_update_ts": "source_update_ts",
        "ingestion_id": "ingestion_id",
        "source_system": "source_system",
        "source_env": "source_env",
        "source_file_path": "source_file_path",
        "load_date": "load_date",
        "processing_ts": "processing_ts",
        "effective_from_ts": "effective_from_ts",
        "effective_to_ts": "effective_to_ts",
        "is_current": "is_current",
    }

    candidates: Dict[str, Column] = {
        "row_hash_id": _row_hash_id(config, ctx),
        "ingestion_run_id": _ingestion_run_id(ctx, pipeline_run_id),
        "processing_ts": F.current_timestamp(),
        "claim_sk": _claim_sk(config, ctx),
        "claim_hk": _claim_hk(config, ctx),
    }

    cols_to_add = {
        col_name: candidates[col_name]
        for flag, col_name in flag_to_column.items()
        if flag in flagged_technicals and col_name in candidates
    }

    df_with_technicals = df_raw.withColumns(cols_to_add) if cols_to_add else df_raw

    # Visual validation (just for us poor humans)
    df_with_technicals.printSchema()
    df_with_technicals.show(1, vertical=True, truncate=120)
    logger.info(
        f"Columns used for hash key: {_retrieve_key_cols(_load_technicals_list(config, ctx), 'hash_key_used_cols')}"
    )
    logger.info(
        f"Columns used for surrogate key: {_retrieve_key_cols(_load_technicals_list(config, ctx), 'surrogate_key_used_cols')}"
    )

    return df_with_technicals
