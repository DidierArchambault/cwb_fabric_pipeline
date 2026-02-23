from bronze_layer.cli.cli import RunContext
from pyspark.sql import DataFrame
import logging

from bronze_layer.bll.helpers.guard import _guard
from bronze_layer.bll.helpers.spark_session import build_spark_session

logger = logging.getLogger(__name__)


def ingest_data(ctx: RunContext, config, path: str) -> DataFrame:
    # Spark configuration
    spark = _guard(
        "building_spark_session",
        lambda: build_spark_session(ctx, session_name="IngestDataJob", max_to_str=1000),
    )

    # Read parquet files
    try:
        logger.info(f"Reading parquet files from {path}")
        df = spark.read.parquet(str(path))
    except Exception as e:
        logger.error(f"Error reading parquet files from {path}: {e}")
        raise e
    # Visual validation (just for us poor humans)
    df.printSchema()
    df.show(1, vertical=True, truncate=120)

    return df
