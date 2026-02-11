from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)


def export_to_delta(ctx, config, df: DataFrame, path: str) -> None:
    df.write.option("overwriteSchema", "true").format("delta").mode("overwrite").save(
        path
    )

    return None
