import logging

from typing import Any, cast
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from bronze_layer.cli.cli import RunContext

logger = logging.getLogger(__name__)


from typing import Any, cast
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


def build_spark_session(
    ctx: RunContext, session_name: str, max_to_str: int
) -> SparkSession:
    """
    Builds a Spark session configured for the bronze layer jobs.
    Handles local dev & tests runs and Fabric runs.
    Args:
        ctx (RunContext): The run context containing configuration information.
        session_name (str): The name of the Spark session.
        max_to_str (int): The maximum number of fields to include in debug string representations.
        Returns:
        SparkSession: A configured Spark session.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        logger.info("Reusing active Spark session.")
        active.conf.set("spark.sql.debug.maxToStringFields", str(max_to_str))
        return active

    base_builder = cast(Any, SparkSession.builder).appName(session_name)
    base_builder = base_builder.config(
        "spark.sql.debug.maxToStringFields", str(max_to_str)
    )

    if ctx.config_source == "local":
        from delta import configure_spark_with_delta_pip

        builder = (
            base_builder.master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()

    if ctx.config_source == "fabric":
        return base_builder.getOrCreate()

    raise ValueError(f"Unknown config source: {ctx.config_source}")
