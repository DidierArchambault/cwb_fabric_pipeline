from __future__ import annotations

from pyspark.sql import SparkSession
import toml


def load_toml_via_spark(spark: SparkSession, path: str) -> dict:
    """
    Load a TOML file via Spark.
        :param spark: Active SparkSession.
        :param path: Path to the TOML file.
        :return: Dictionary representation of the TOML file.
    """
    if spark is None:
        raise RuntimeError(
            "No active SparkSession. This loader must run inside a Spark runtime."
        )

    rows = spark.read.text(path).collect()
    txt = "\n".join(r.value for r in rows)
    return toml.loads(txt)
