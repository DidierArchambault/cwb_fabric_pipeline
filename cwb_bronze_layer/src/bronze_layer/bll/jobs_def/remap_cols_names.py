import logging
import json


from importlib import resources
from typing import Dict, List
from pyspark.sql import DataFrame
from urllib.parse import urlparse

from bronze_layer.resources.config_bz.configuration import Configuration
from bronze_layer.bll.helpers.spark_session import build_spark_session
from bronze_layer.cli.cli import RunContext

logger = logging.getLogger(__name__)


def _read_text_via_spark(spark, path_str: str) -> str:
    df = spark.read.text(path_str)
    lines = [r["value"] for r in df.collect()]
    return "\n".join(lines)


def _load_rename_map(config: Configuration, ctx: RunContext) -> Dict:
    path_str = config.rules_configuration.rename_map_json
    logger.info("Loading rename map JSON from: %s", path_str)

    if ctx.config_source == "local":
        from pathlib import Path

        return json.loads(Path(path_str).read_text(encoding="utf-8"))

    if ctx.config_source == "fabric":
        parsed = urlparse(path_str)
        if parsed.scheme in ("abfs", "abfss"):
            spark = build_spark_session(
                ctx, "rename_cols_names_json_loader", max_to_str=1000
            )
            return json.loads(_read_text_via_spark(spark, path_str))
        else:
            from pathlib import Path

            return json.loads(Path(path_str).read_text(encoding="utf-8"))

    raise ValueError(f"Unknown config source: {ctx.config_source}")


def _build_cols_rename_map(df_cols: List[str], rename_map: Dict) -> Dict[str, str]:
    """
    Returns a dict {col_name: new_col_name} depending on the columns_to_rename in the JSON.
    """
    cols_list_rename = rename_map.get("columns_to_rename", {})

    return {
        og_col_name: new_col_name
        for og_col_name, new_col_name in cols_list_rename.items()
        if og_col_name in df_cols
    }


def rename_cols(config: Configuration, df: DataFrame, ctx: RunContext) -> DataFrame:
    """
    Renames columns of the DataFrame based on the configuration.
    Specify the columns to rename and their new names in the JSON file (bronze_rename_map.json)
    Args:
        config: The configuration object containing columns rename settings.
        df: The input DataFrame whose columns will be renamed.
    Returns:
        DataFrame: A new DataFrame with renamed columns.
    """

    rename_map_json = _load_rename_map(config, ctx)

    df_in = df
    df_cols_list = df_in.columns

    cols_to_rename = _build_cols_rename_map(df_cols_list, rename_map_json)
    rename_map_final: Dict[str, str] = {}

    # On part de la liste des cols du DF_in. Pour chaque col,
    # on regarde si elle est dans le mapping de renommage,
    # si oui, on ajoute la paire (col, new_col) dans rename_map_final
    for col in df_cols_list:
        new_name = cols_to_rename.get(col)
        if not new_name:
            continue
        if new_name != col:
            rename_map_final[col] = new_name

    if not rename_map_final:
        return df_in

    new_cols = [rename_map_final.get(c, c) for c in df_cols_list]
    df_out = df_in.toDF(*new_cols)

    logger.info("Renamed columns: %s", rename_map_final)

    return df_out
