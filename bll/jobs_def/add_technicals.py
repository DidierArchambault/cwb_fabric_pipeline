from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from typing import Dict
import logging


logger = logging.getLogger(__name__)

def add_technicals(ctx, config, df: DataFrame) -> DataFrame:
    # Load tecnical configuration (contains which technical columns to add)
    tech_cfg = config.technicals_configuration
    # raw DataFrame
    df_raw = df
    # possible columns to add
    candidates: Dict[str, Column] = {
        "row_hash_id": F.sha2(
            F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df.columns]),
            256,
        ),
        "ingestion_run_id": F.lit(getattr(ctx, "run_id", "test_id")),
        "schema_id": F.lit(getattr(ctx, "schema_id", "unknown")), # to be improved depending on what we want
        "ingest_timestamp": F.current_timestamp(),
    }
    cols_to_add = {
            "ingestion_date": F.current_date(),
            "ingestion_run_id": F.lit("test_id"),
        }
    flag_to_column = {
        "Row_Hash_ID": "row_hash_id",
        "Ingestion_Run_ID": "ingestion_run_id",
        "Schema_ID": "schema_id",
        "Ingest_Timestamp": "ingest_timestamp",
    }
    cols_to_add = {
        col_name: candidates[col_name]
        for flag, col_name in flag_to_column.items()
        if getattr(tech_cfg, flag, False)
    }

    df_with_technicals = df_raw.withColumns(cols_to_add) if cols_to_add else df_raw

    # Visual validation (just for us poor humans)
    df_with_technicals.printSchema()
    df_with_technicals.show(1, vertical=True, truncate=120)

    return df_with_technicals