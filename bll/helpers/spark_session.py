from typing import Any, cast
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

import logging

logger = logging.getLogger(__name__)

def build_spark_session(session_name: str, max_to_str: int) -> SparkSession:
    base_builder = cast(Any, SparkSession.builder)
    builder = (
        base_builder
        .appName(session_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.debug.maxToStringFields", str(max_to_str))
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()
