import logging
import json

from pathlib import Path
from typing import Dict, List
from pyspark.sql import DataFrame

from configuration.configuration import Configuration


logger = logging.getLogger(__name__)


def _load_rename_map(config: Configuration) -> Dict:
    """JSON columns name maping."""
    rename_map = Path(config.bronze_configuration.columns_rename_map)
    with open(rename_map, "r") as f:
        return json.load(f)


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


def rename_cols(
    config: Configuration, df: DataFrame
) -> DataFrame:
    """
    Renames columns of the DataFrame based on the configuration.
    Specify the columns to rename and their new names in the JSON file (bronze_rename_map.json)
    Args:
        config: The configuration object containing columns rename settings.
        df: The input DataFrame whose columns will be renamed.
    Returns:
        DataFrame: A new DataFrame with renamed columns.
    """

    rename_map_json = _load_rename_map(config)

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
