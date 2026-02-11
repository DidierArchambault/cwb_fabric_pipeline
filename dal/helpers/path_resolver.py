from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def resolve_path(config, ctx, req_path: str) -> Path:
    """
    Resolves the path for a given context based on the configuration and context parameters.\n
    :param config: Configuration object containing path templates.
    :param ctx: RunContext object containing parameters for path resolution.
    :return: Resolved path as a string.
    """
    try:
        if req_path == "landing":
            return Path(
                config.landing_zone_configuration.base_path
            ) / config.landing_zone_configuration.path_layout.format(
                source="landing",
                x_center="claimcenter",
                step="initial",
                status="ready",
                table="cc_claim",
            )

        if req_path == "deltalake":
            return (
                Path(config.landing_zone_configuration.base_path).parents[2]
                / "temp_sink"
                / config.landing_zone_configuration.path_layout.format(
                    source="DeltaLake",
                    x_center="claimcenter",
                    step="initial",
                    status="ready",
                    table="cc_claim",
                )
            )
        raise ValueError(f"Unknown path request: {req_path}")

    except Exception as e:
        logger.error(f"Error resolving path for request {req_path}: {e}")
        raise e
