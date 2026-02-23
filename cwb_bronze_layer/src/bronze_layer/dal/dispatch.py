import argparse
import logging

from bronze_layer.resources.config_bz.configuration import Configuration
from bronze_layer.cli.cli import RunContext

logger = logging.getLogger(__name__)


def dispatch(ctx: RunContext, config: Configuration, pipeline_run_id: str) -> bool:
    """
    Dispatches the command to the appropriate handler based on parsed arguments.
    Individual run will be called for single layer ops.
    pipeline.run.run will be called for "all" command.\n
    :param ctx: RunContext object containing parsed command-line arguments.
    :param config: Configuration object.
    :return: True if a layer ran successfully, False otherwise.
    """
    logger.info("Starting pipeline run for bronze layer.")
    logger.info("Selected work is: %s.", ctx.work)

    try:
        from bronze_layer.bll.jobs_luncher.bronze_launch import bronze_launch

        result = bool(
            bronze_launch(ctx=ctx, config=config, pipeline_run_id=pipeline_run_id)
        )
        logger.info("Layer 'bronze' finished with success=%s", result)
        return result

    except Exception as e:
        logger.exception("Error running bronze layer: %s", e)
        return False

    finally:
        logger.info(f"Bronze layer operations finished for {ctx.xcenter}")
