from dataclasses import dataclass


@dataclass
class BronzeConfiguration:
    """
    Configuration for the bronze stage of the pipeline.
    """
    db_path: str



def from_dict(data: dict) -> "BronzeConfiguration":
    """
    Create a BronzeConfiguration instance from a dictionary.
    """
    return BronzeConfiguration(
        db_path=data.get("db_path", ""),

    )


def to_dict(data: BronzeConfiguration) -> dict:
    """
    Convert the BronzeConfiguration instance to a dictionary.
    """
    return {
        "db_path": data.db_path,

    }
