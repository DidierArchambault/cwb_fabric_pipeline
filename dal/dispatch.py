import argparse
import logging

from configuration.configuration import Configuration
from dal.cli.cli import RunContext

import bll.jobs_luncher.bronze_lunch
import bll.jobs_luncher.silver_lunch
import bll.jobs_luncher.gold_lunch
import tests


logger = logging.getLogger(__name__)

# la logique va changer
# on va execute par couche encore, mais on va faire un dispatch ici
# et appeler les bons handlers
def dispatch(args: argparse.Namespace, config: Configuration, pipeline_run_id:str) -> bool:
    """
    Dispatches the command to the appropriate handler based on parsed arguments.
    Individual run will be called for single layer ops.
    pipeline.run.run will be called for "all" command.\n
    :param args: Parsed command-line arguments.
    :param config: Configuration object.
    :return: True if a handler ran successfully, False otherwise.
    """

    ctx = RunContext(
        env=getattr(args, "env", None),
        layer=getattr(args, "layer", None),
        work=getattr(args, "work", None),
        semantic_one=getattr(args, "semantic_one", None),
        xcenter=getattr(args, "xcenter", None),
    )

    print("\n")
    logger.info("Starting pipeline run for %s.", ctx.layer)
    logger.info("Selected work is: %s.", ctx.work)
    print("\n")

    if not hasattr(args, "handler"):
        logger.error("No handler associated with command")
        return False

    try:
        if args.handler == "bronze_run":
            from bll.jobs_luncher.bronze_lunch import bronze_lunch
            result = bool(bronze_lunch(ctx=ctx, config=config, pipeline_run_id=pipeline_run_id))
            logger.info("Handler 'bronze_run' finished with success=%s", result)
            return result

        # elif args.handler == "silver_run":
        #     from bsgp._silver.scripts.run.run import run as silver_run
        #     return bool(silver_run(ctx=ctx, config=config))

        # elif args.handler == "gold_run":
        #     from bsgp._gold.scripts.run.run import run as gold_run
        #     return bool(gold_run(ctx=ctx, config=config))

        # elif args.handler == "all_run":
        #     from pipeline.run import run as all_run
        #     return bool(all_run(ctx=ctx, config=config))

        else:
            logger.error("Unknown handler: %s", args.handler)
            return False

    finally:
        # if ctx.clean_after and args.handler == "all_run":
        #     _clean_bsg_medaillon(config)
            logger.info("Last step for loop pliz change ;).")

