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
        if ctx.config_source == "local":
            if req_path == "landing":
                return Path(
                    config.storage_configuration.landing_base
                ) / config.storage_configuration.landing_layout.format(
                    x_center="claimcenter",
                    step="initial_load",
                    status="ready",
                    table="cc_claim",
                )

            if req_path == "_temp_sink":
                return Path(
                    config.storage_configuration.bronze_db
                ) / config.storage_configuration.landing_layout.format(
                    x_center="claimcenter",
                    step="initial_load",
                    status="ready",
                    table="cc_claim",
                )

            else:
                raise ValueError(f"Unknown path request: {req_path}")

        elif ctx.config_source == "fabric":
            if req_path == "landing":
                return Path(
                    config.storage_configuration.landing_base
                ) / config.storage_configuration.landing_layout.format(
                    x_center="claimcenter",
                    step="initial_load",
                    status="ready",
                    table="cc_claim",
                )

            if req_path == "bronze_db":
                return Path(config.storage_configuration.bronze_db)
            else:
                raise ValueError(f"Unknown path request: {req_path}")

        else:
            raise ValueError(f"Unknown config source: {ctx.config_source}")

    except Exception as e:
        logger.error(f"Error resolving path for request {req_path}: {e}")
        raise e


def resolve_under_root(root: str, path: str) -> str:
    """
    If `path` is relative, resolve it under `root`.
    If it's absolute, return as-is.
    If it's an abfss path, return as-is.
    """
    if path.startswith("abfss://"):
        return path

    p = Path(path)
    if p.is_absolute():
        return str(p)

    return str(Path(root) / p)
