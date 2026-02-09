from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from typing import Dict
from datetime import date, datetime
import logging


logger = logging.getLogger(__name__)

def export_to_delta(ctx, config, df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("overwrite").save(path)

    return None