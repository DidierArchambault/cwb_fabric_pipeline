from dataclasses import dataclass

import configuration.landing_zone_configuration
import configuration.bronze_configuration
import configuration.gold_configuration
import configuration.silver_configuration
import configuration.tests_configuration
import configuration.technicals_configuration

@dataclass
class Configuration:
    """
    Configuration for the pipeline run.
    """
    landing_zone_configuration: configuration.landing_zone_configuration.LandingZoneConfiguration
    bronze_configuration: configuration.bronze_configuration.BronzeConfiguration
    silver_configuration: configuration.silver_configuration.SilverConfiguration
    gold_configuration: configuration.gold_configuration.GoldConfiguration
    tests_configuration: configuration.tests_configuration.TestsConfiguration
    technicals_configuration: configuration.technicals_configuration.TechnicalsConfiguration

def from_dict(data: dict) -> "Configuration":
    """
    Create a Configuration instance from a dictionary.
    """
    return Configuration(
        landing_zone_configuration=configuration.landing_zone_configuration.from_dict(
            data.get("landing_zone", {})
        ),
        bronze_configuration=configuration.bronze_configuration.from_dict(
            data.get("bronze", {})
        ),
        silver_configuration=configuration.silver_configuration.from_dict(
            data.get("silver", {})
        ),
        gold_configuration=configuration.gold_configuration.from_dict(
            data.get("gold", {})
        ),
        tests_configuration=configuration.tests_configuration.from_dict(
            data.get("tests", {})
        ),
        technicals_configuration=configuration.technicals_configuration.from_dict(
            data.get("technicals", {})
        ),
    )


def to_dict(data: Configuration) -> dict:
    """
    Convert the Configuration instance to a dictionary.
    """
    return {
        "landing_zone": configuration.landing_zone_configuration.to_dict(data.landing_zone_configuration),
        "bronze": configuration.bronze_configuration.to_dict(data.bronze_configuration),
        "silver": configuration.silver_configuration.to_dict(data.silver_configuration),
        "gold": configuration.gold_configuration.to_dict(data.gold_configuration),
        "tests": configuration.tests_configuration.to_dict(data.tests_configuration),
        "technicals": configuration.technicals_configuration.to_dict(data.technicals_configuration),
    }
