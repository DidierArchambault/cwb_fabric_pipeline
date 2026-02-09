from dataclasses import dataclass


@dataclass
class SilverConfiguration:
    """
    Configuration for the silver stage of the pipeline.
    """

    db_path: str
    scripts_folder_path: str
    run_one_script_path: str
    index_path: str
    parquets_path_in: str
    parquets_path_out: str
    output_path: str


def from_dict(data: dict) -> "SilverConfiguration":
    """
    Create a SilverConfiguration instance from a dictionary.
    """
    return SilverConfiguration(
        db_path=data.get("db_path", ""),
        scripts_folder_path=data.get("scripts_folder_path", ""),
        run_one_script_path=data.get("run_one_script_path", ""),
        index_path=data.get("index_path", ""),
        parquets_path_in=data.get("parquets_path_in", ""),
        parquets_path_out=data.get("parquets_path_out", ""),
        output_path=data.get("output_path", ""),
    )


def to_dict(data: SilverConfiguration) -> dict:
    """
    Convert the SilverConfiguration instance to a dictionary.
    """
    return {
        "db_path": data.db_path,
        "scripts_folder_path": data.scripts_folder_path,
        "run_one_script_path": data.run_one_script_path,
        "index_path": data.index_path,
        "parquets_path_in": data.parquets_path_in,
        "parquets_path_out": data.parquets_path_out,
        "output_path": data.output_path,
    }
