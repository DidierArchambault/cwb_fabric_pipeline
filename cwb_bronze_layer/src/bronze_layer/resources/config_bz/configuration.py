from dataclasses import dataclass

import bronze_layer.resources.config_bz.execution_configuration
import bronze_layer.resources.config_bz.storage_configuration
import bronze_layer.resources.config_bz.rules_configuration
import bronze_layer.resources.config_bz.observability_configuration


@dataclass
class Configuration:
    """
    Configuration for the pipeline run.
    """

    execution_configuration: (
        bronze_layer.resources.config_bz.execution_configuration.ExecutionConfiguration
    )
    storage_configuration: (
        bronze_layer.resources.config_bz.storage_configuration.StorageConfiguration
    )
    rules_configuration: (
        bronze_layer.resources.config_bz.rules_configuration.RulesConfiguration
    )
    observability_configuration: (
        bronze_layer.resources.config_bz.observability_configuration.ObservabilityConfiguration
    )


def from_dict(data: dict) -> "Configuration":
    """
    Create a Configuration instance from a dictionary.
    """
    return Configuration(
        execution_configuration=bronze_layer.resources.config_bz.execution_configuration.from_dict(
            data.get("execution", {})
        ),
        storage_configuration=bronze_layer.resources.config_bz.storage_configuration.from_dict(
            data.get("storage", {})
        ),
        rules_configuration=bronze_layer.resources.config_bz.rules_configuration.from_dict(
            data.get("rules", {})
        ),
        observability_configuration=bronze_layer.resources.config_bz.observability_configuration.from_dict(
            data.get("observability", {})
        ),
    )


def to_dict(data: Configuration) -> dict:
    """
    Convert the Configuration instance to a dictionary.
    """
    return {
        "execution": bronze_layer.resources.config_bz.execution_configuration.to_dict(
            data.execution_configuration
        ),
        "storage": bronze_layer.resources.config_bz.storage_configuration.to_dict(
            data.storage_configuration
        ),
        "rules": bronze_layer.resources.config_bz.rules_configuration.to_dict(
            data.rules_configuration
        ),
        "observability": bronze_layer.resources.config_bz.observability_configuration.to_dict(
            data.observability_configuration
        ),
    }
