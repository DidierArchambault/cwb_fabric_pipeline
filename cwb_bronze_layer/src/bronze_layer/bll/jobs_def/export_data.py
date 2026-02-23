from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)


def export_to_delta(ctx, config, df: DataFrame, path: str) -> None:
    """
    Exports the given DataFrame to a Delta Lake location specified by the path.
    The export behavior depends on the configuration source (local or fabric).
    Fabric does not need path. The attached lakehouse will be used, we must simply
    specify the table name in saveAsTable. Local will use the provided path to write delta files.
    Args:
        ctx (RunContext): The run context containing configuration information.
        config (Configuration): The configuration object containing path templates.
        df (DataFrame): The DataFrame to export.
        path (str): The target path for the Delta Lake export.
    Returns:
        None
    """
    if ctx.config_source == "local":
        logger.info(f"Exporting DataFrame to local Delta Lake at {path}")
        (
            df.write.option("mergeSchema", "true")
            .format("delta")
            .mode("append")
            .save(path)
        )

    elif ctx.config_source == "fabric":
        logger.info(f"Exporting DataFrame to fabric Delta Lake at {path}")
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable("cc_claim")
        )

    else:
        raise ValueError(f"Unknown config source: {ctx.config_source}")

    return None
