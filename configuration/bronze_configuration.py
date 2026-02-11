from dataclasses import dataclass


@dataclass
class BronzeConfiguration:
    """
    Configuration for the bronze stage of the pipeline.
    """

    db_path: str
    path_layout: str
    columns_to_clean_rules: str
    columns_rename_map: str
    bronze_technicals: str


def from_dict(data: dict) -> "BronzeConfiguration":
    """
    Create a BronzeConfiguration instance from a dictionary.
    """
    return BronzeConfiguration(
        db_path=data.get("db_path", ""),
        path_layout=data.get("path_layout", ""),
        columns_to_clean_rules=data.get("columns_to_clean_rules", ""),
        columns_rename_map=data.get("columns_rename_map", ""),
        bronze_technicals=data.get("bronze_technicals", ""),
    )


def to_dict(data: BronzeConfiguration) -> dict:
    """
    Convert the BronzeConfiguration instance to a dictionary.
    """
    return {
        "db_path": data.db_path,
        "path_layout": data.path_layout,
        "columns_to_clean_rules": data.columns_to_clean_rules,
        "columns_rename_map": data.columns_rename_map,
        "bronze_technicals": data.bronze_technicals,
    }
