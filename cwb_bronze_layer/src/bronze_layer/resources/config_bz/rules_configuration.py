from dataclasses import dataclass


@dataclass
class RulesConfiguration:
    """
    Configuration for the rules used in the pipeline.
    """

    rule_set_id: str
    cleaning_json: str
    rename_map_json: str
    technicals_json: str


def from_dict(data: dict) -> "RulesConfiguration":
    """
    Create a RulesConfiguration instance from a dictionary.
    """
    return RulesConfiguration(
        rule_set_id=data.get("rule_set_id", False),
        cleaning_json=data.get("cleaning_json", False),
        rename_map_json=data.get("rename_map_json", False),
        technicals_json=data.get("technicals_json", False),
    )


def to_dict(data: RulesConfiguration) -> dict:
    """
    Convert the RulesConfiguration instance to a dictionary.
    """
    return {
        "rule_set_id": data.rule_set_id,
        "cleaning_json": data.cleaning_json,
        "rename_map_json": data.rename_map_json,
        "technicals_json": data.technicals_json,
    }
