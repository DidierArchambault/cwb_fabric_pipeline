from dataclasses import dataclass


@dataclass
class ExecutionConfiguration:
    """
    Configuration for the ExecutionConfiguration used for high level usage config.
    """

    base_path: str
    path_layout: str


def from_dict(data: dict) -> "ExecutionConfiguration":
    """
    Create a ExecutionConfiguration instance from a dictionary.
    """
    return ExecutionConfiguration(
        base_path=data.get("base_path", ""),
        path_layout=data.get("path_layout", ""),
    )


def to_dict(data: ExecutionConfiguration) -> dict:
    """
    Convert the ExecutionConfiguration instance to a dictionary.
    """
    return {
        "base_path": data.base_path,
        "path_layout": data.path_layout,
    }
