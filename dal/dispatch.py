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
def dispatch(args: argparse.Namespace, config: Configuration) -> bool:
    """
    Dispatches the command to the appropriate handler based on parsed arguments.
    Individual run will be called for single layer ops.
    pipeline.run.run will be called for "all" command.\n
    :param args: Parsed command-line arguments.
    :param config: Configuration object.
    :return: None
    """
    ctx = RunContext(
        layer=getattr(args, "layer", None),
        work=getattr(args, "work", None),
        semantic_one=getattr(args, "semantic_one", None),
    )

    print("\n")
    logger.info("Starting pipeline run for %s.", ctx.layer)
    logger.info("Selected work is: %s.", ctx.work)
    print("\n")

    if not hasattr(args, "handler"):
        raise RuntimeError("No handler associated with command")

    try:
        if args.handler == "bronze_run":
            from bll.jobs_luncher.bronze_lunch import  bronze_lunch
            return bool(bronze_lunch(ctx=ctx, config=config))

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
            raise RuntimeError(f"Unknown handler: {args.handler}")

    finally:
        # if ctx.clean_after and args.handler == "all_run":
        #     _clean_bsg_medaillon(config)
            logger.info("Last step for loop pliz change ;).")

