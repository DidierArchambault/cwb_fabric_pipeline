from configuration.configuration import Configuration
from dal.cli.cli import RunContext


def silver_lunch(ctx: RunContext, config: Configuration) -> None:
    """
    Launch the silver layer job to merge parquet files.

    Args:
        parquets_path (str): The path to the directory containing parquet files.
    """
    # Add spark jobs call here
    # Give things needed by the scripts from ctx and config
    pass
