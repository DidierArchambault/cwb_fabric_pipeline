from __future__ import annotations

import argparse
import logging

from dataclasses import dataclass
from typing import Any, Literal

from bronze_layer.cli.help_table.help_table_cli import HELP_TABLE

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunContext:
    env: str | None
    layer: str | None
    cmd: str | None
    work: str | None
    xcenter: str | None = None

    config_source: Literal["local", "fabric"] | None = None
    config_path: Any | None = None


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="bronze_layer",
        description="Runner for Fabric Spark Jobs (usable locally).",
        epilog=HELP_TABLE,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    p.add_argument("--env", "-e", choices=["prod", "dev", "qa"], default="dev")
    p.add_argument(
        "--config",
        "-c",
        choices=["fabric", "local"],
        dest="config_source",
        default="local",
    )
    p.add_argument("--config-path", "-cp", dest="config_path")
    p.add_argument("--xcenter", "-xc", choices=["AB", "BC", "CC", "PC"], default="CC")
    p.add_argument(
        "--work",
        "-w",
        choices=["all", "ingest_data", "add_technicals", "export_to_delta"],
        default="all",
    )
    p.set_defaults(handler="bronze_run")

    return p
