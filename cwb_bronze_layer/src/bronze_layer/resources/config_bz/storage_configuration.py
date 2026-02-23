from dataclasses import dataclass


@dataclass
class StorageConfiguration:
    """
    Configuration for the storage elements in the pipeline.
    """

    landing_base: str
    landing_layout: str
    bronze_db: str
    bronze_files: str


def from_dict(data: dict) -> "StorageConfiguration":
    """
    Create a StorageConfiguration instance from a dictionary.
    """
    return StorageConfiguration(
        landing_base=data.get("landing_base", ""),
        landing_layout=data.get("landing_layout", ""),
        bronze_db=data.get("bronze_db", ""),
        bronze_files=data.get("bronze_files", ""),
    )


def to_dict(data: StorageConfiguration) -> dict:
    """
    Convert the StorageConfiguration instance to a dictionary.
    """
    return {
        "landing_base": data.landing_base,
        "landing_layout": data.landing_layout,
        "bronze_db": data.bronze_db,
        "bronze_files": data.bronze_files,
    }
