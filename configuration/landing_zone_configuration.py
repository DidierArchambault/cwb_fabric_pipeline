from dataclasses import dataclass


@dataclass
class LandingZoneConfiguration:
    """
    Configuration for the landing zone stage of the pipeline.
    """
    base_path: str
    path_layout: str


def from_dict(data: dict) -> "LandingZoneConfiguration":
    """
    Create a LandingZoneConfiguration instance from a dictionary.
    """
    return LandingZoneConfiguration(
        base_path=data.get("base_path", ""),
        path_layout=data.get("path_layout", ""),
    )


def to_dict(data: LandingZoneConfiguration) -> dict:
    """
    Convert the LandingZoneConfiguration instance to a dictionary.
    """
    return {
        "base_path": data.base_path,
        "path_layout": data.path_layout,
    }
