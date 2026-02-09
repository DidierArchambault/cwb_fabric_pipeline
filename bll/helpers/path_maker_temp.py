from pathlib import Path
from configuration.configuration import Configuration

config = Configuration


def build_landing_path(config: Configuration) -> Path:
    return (
        Path(config.landing_zone_configuration.base_path)
        / config.landing_zone_configuration.layout.format(
            source="landing",
            x_center="claimcenter",
            step="initial",
            status="ready",
            table="cc_claim",
        )
    )

def build_deltalake_path(config: Configuration) -> Path:
    return (
        Path(config.landing_zone_configuration.base_path).parents[2]
        / "temp_sink"
        / config.landing_zone_configuration.layout.format(
            source="DeltaLake",
            x_center="claimcenter",
            step="initial",
            status="ready",
            table="cc_claim",
        )
    )

def build_outlogs_path(config: Configuration) -> Path:
    return (
        Path(config.landing_zone_configuration.base_path)
        / config.landing_zone_configuration.layout.format(
            source="DeltaLake",
            x_center="claimcenter",
            step="initial",
            status="ready",
            table="cc_claim",
        )
    )