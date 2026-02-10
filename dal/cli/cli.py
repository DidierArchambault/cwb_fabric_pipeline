from __future__ import annotations
import argparse
import logging
from dataclasses import dataclass

from dal.cli.help_table.help_table_cli import HELP_TABLE

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunContext:
    env: str | None
    layer: str | None
    work: str | None
    semantic_one: str | None
    xcenter: str | None = None


def build_parser() -> argparse.ArgumentParser:
    # Parent: common options for the 3 layers
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--env", "-e",
        choices=["prod", "dev", "qa"],
        default="dev",
        help="Environment selector / Define the environment to run on",
    )
    p = argparse.ArgumentParser(
        prog="SparkJobRunner",
        description="Runner for Fabric SparkJobs",
        epilog=HELP_TABLE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    layers = p.add_subparsers(dest="layer", required=True)


    # ----------------------------
    # --- BRONZE LAYER PARSING ---
    # ----------------------------
    bronze = layers.add_parser("bronze", help="Bronze layer commands")
    bronze_cmds = bronze.add_subparsers(dest="cmd", required=True)

    bronze_run = bronze_cmds.add_parser(
        "run",
        help="Run bronze layer",
        parents=[common],
    )
    bronze_run.add_argument(
        "--xcenter", "-xc",
        choices=["AB", "BC", "CC", "PC"],
        default="CC",
        help="Bronze X-Center selector / Define the xcenter to run on",
    )
    bronze_run.add_argument(
        "--work", "-w",
        choices=[
            "all"
            "ingest_data",
            "add_technicals",
            "export_to_delta",
        ],
        default="all",
        help="Choose which step to run in bronze",
    )
    bronze_run.set_defaults(handler="bronze_run")


    # ----------------------------
    # --- SILVER LAYER PARSING ---
    # ----------------------------
    silver = layers.add_parser("silver", help="Silver layer commands")
    silver_cmds = silver.add_subparsers(dest="cmd", required=True)

    silver_run = silver_cmds.add_parser(
        "run",
        help="Run silver layer",
        parents=[common],
    )
    silver_run.add_argument(
        "--work", "-w",
        choices=["all", "run_one_script"],
        default="all",
        help="Silver scope selector / Define the single script to run in config file",
    )
    silver_run.set_defaults(handler="silver_run")


    # --------------------------
    # --- GOLD LAYER PARSING ---
    # --------------------------
    gold = layers.add_parser("gold", help="Gold layer commands")
    gold_cmds = gold.add_subparsers(dest="cmd", required=True)

    gold_run = gold_cmds.add_parser(
        "run",
        help="Run gold layer",
        parents=[common],
    )
    gold_run.add_argument(
        "--semantic",
        choices=["facts", "dims", "bridge", "all"],
        default="all",
        help="Gold scope selector / Select which part of the semantic layer to run",
    )
    gold_run.add_argument(
        "--work", "-w",
        choices=["all", "run_one_script"],
        default="all",
        help="Gold scope selector / Define the single script to run in config file",
    )
    gold_run.add_argument(
        "--xcenter", "-xc",
        choices=["AB", "BC", "CC", "PC"],
        default="CC",
        help="Gold X-Center selector / Define the xcenter to run on",
    )
    gold_run.add_argument(
        "--semantic_one", "-so",
    choices=["fact_group_1", "fact_group_2", "fact_group_3", "dim_group_1", "dim_group_2"],
    default="all",
    help="If --semantic_one facts: select one facts family to run",
)
    gold_run.set_defaults(handler="gold_run")

    return p
