from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Row
from pathlib import Path
from datetime import date, datetime
import logging

from bll.helpers.guard import _guard
from bll.helpers.spark_session import build_spark_session


logger = logging.getLogger(__name__)




def ingest_data(ctx, config, path: str) -> DataFrame:
    # Spark configuration
    spark = _guard("building_spark_session",
                   lambda:build_spark_session(session_name="IngestDataJob", max_to_str=1000)
                   )

    # Read parquet files
    df = spark.read.parquet(str(path))
    # Visual validation (just for us poor humans)
    df.printSchema()
    df.show(1, vertical=True, truncate=120)

    return df