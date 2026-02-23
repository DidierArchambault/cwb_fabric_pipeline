import logging
import json
from urllib.parse import urlparse

from typing import Dict, List
from bronze_layer.bll.helpers.spark_session import build_spark_session
from bronze_layer.cli.cli import RunContext
from pyspark.sql import DataFrame

from bronze_layer.bll.helpers.cleaning_utils.normalize_case import normalize_case
from bronze_layer.resources.config_bz.configuration import Configuration

from bronze_layer.bll.helpers.cleaning_utils.normalize_naming_conv import (
    normalize_naming_conv,
)
from bronze_layer.bll.helpers.cleaning_utils.remove_space import space_remover

logger = logging.getLogger(__name__)


def _read_text_via_spark(spark, path_str: str) -> str:
    df = spark.read.text(path_str)
    lines = [r["value"] for r in df.collect()]
    return "\n".join(lines)


def _load_cleaning_specs(config: Configuration, ctx: RunContext) -> Dict:
    path_str = config.rules_configuration.cleaning_json
    logger.info("Loading cleaning rules JSON from: %s", path_str)

    if ctx.config_source == "local":
        from pathlib import Path

        return json.loads(Path(path_str).read_text(encoding="utf-8"))

    if ctx.config_source == "fabric":
        parsed = urlparse(path_str)
        if parsed.scheme in ("abfs", "abfss"):
            spark = build_spark_session(
                ctx, "clean_cols_names_json_loader", max_to_str=1000
            )
            return json.loads(_read_text_via_spark(spark, path_str))
        else:
            from pathlib import Path

            return json.loads(Path(path_str).read_text(encoding="utf-8"))

    raise ValueError(f"Unknown config source: {ctx.config_source}")


def _build_cols_steps_map(
    df_cols: List[str], cleaning_rules: Dict
) -> Dict[str, List[str]]:
    """
    Returns a dict {col_name: [steps,...]} depending on the all_flag flag.

    - if all_flag.clean == True: apply these steps to all columns
        and ignore the rest
    - otherwise: only take columns_list.* where clean == True
    """
    cols_cfg = cleaning_rules.get("columns_to_clean", {})

    all_flag_cfg = cols_cfg.get("all_flag", {})
    if all_flag_cfg.get("clean"):
        steps = all_flag_cfg.get("steps", [])
        return {c: steps for c in df_cols}

    cols_list_cfg = cols_cfg.get("columns_list", {})
    cols_steps: Dict[str, List[str]] = {}

    for col_name, spec in cols_list_cfg.items():
        if not spec.get("clean"):
            continue
        if col_name not in df_cols:
            logger.debug(
                "Column '%s' specified in cleaning rules but not found in DF", col_name
            )
            continue
        cols_steps[col_name] = spec.get("steps", [])

    return cols_steps


def _apply_steps_to_col_name(col_name: str, steps: List[str]) -> str:
    """
    Applies the steps sequentially to the column name (string).

    Possible steps in your current JSON:
      - "remove_space": uses space_remover
      - "snake" / "kebab" / "pascal" / "camel": uses normalize_naming_conv
    """
    # TODO: ADD MORE STEPS AS NEEDED
    value = col_name
    for step in steps:
        if step == "remove_space":
            value = space_remover(value)
        elif step in ("snake", "kebab", "pascal", "camel"):
            value = normalize_naming_conv(value, step)
        elif step == "capitalize":
            value = normalize_case(value, "capitalize")
        elif step == "lower":
            value = normalize_case(value, "lower")
        elif step == "upper":
            value = normalize_case(value, "upper")
        else:
            logger.warning("Unknown cleaning step '%s' for column '%s'", step, col_name)
    return value


def clean_col_names(config: Configuration, df: DataFrame, ctx: RunContext) -> DataFrame:
    """
    Cleans the column names of the DataFrame according to the JSON rules.

    - Uses JSON directly (no complicated remodelling)
    - If all_flag.clean == True: applies its steps to all columns
        and ignores columns_list
    - Otherwise: for each column in columns_list with clean == True,
      applies its steps\n
    Available rules (defined in the JSON):
    - "remove_space": removes spaces from column names
    - "snake": converts to snake_case
    - "kebab": converts to kebab-case
    - "pascal": converts to PascalCase
    - "camel": converts to camelCase
    - "capitalize": capitalizes the first letter of each word
    - "lower": converts all letters to lowercase
    - "upper": converts all letters to uppercase
    Args:
        config: The configuration object containing the path to cleaning rules.
        df: The input DataFrame with original column names.
    Returns: A new DataFrame with cleaned column names according to the specified rules.
    """

    cleaning_rules = _load_cleaning_specs(config, ctx)

    df_in = df
    df_cols_list = df_in.columns

    cols_steps = _build_cols_steps_map(df_cols_list, cleaning_rules)

    rename_map: Dict[str, str] = {}
    for col in df_cols_list:
        steps = cols_steps.get(col)
        if not steps:
            logger.info("No cleaning steps defined for column '%s'. Skipping.", col)
            continue
        new_col_name = _apply_steps_to_col_name(col, steps)
        if new_col_name != col:
            rename_map[col] = new_col_name

    if not rename_map:
        return df_in

    new_cols = [rename_map.get(c, c) for c in df_cols_list]
    df_out = df_in.toDF(*new_cols)

    logger.info("Renamed columns: %s", rename_map)

    return df_out
