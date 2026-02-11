from dataclasses import dataclass


@dataclass
class GoldConfiguration:
    """
    Configuration for the gold stage of the pipeline.
    """

    db_path: str


def from_dict(data: dict) -> "GoldConfiguration":
    """
    Create a GoldConfiguration instance from a dictionary.
    """
    return GoldConfiguration(
        db_path=data.get("db_path", ""),
    )


def to_dict(data: GoldConfiguration) -> dict:
    """
    Convert the GoldConfiguration instance to a dictionary.
    """
    return {
        "db_path": data.db_path,
    }
