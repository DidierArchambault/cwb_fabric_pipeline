from dataclasses import dataclass


@dataclass
class LandingZoneConfiguration:
    """
    Configuration for the landing zone stage of the pipeline.
    """
    base_path: str
    layout: str


def from_dict(data: dict) -> "LandingZoneConfiguration":
    """
    Create a LandingZoneConfiguration instance from a dictionary.
    """
    return LandingZoneConfiguration(
        base_path=data.get("base_path", ""),
        layout=data.get("layout", ""),
    )


def to_dict(data: LandingZoneConfiguration) -> dict:
    """
    Convert the LandingZoneConfiguration instance to a dictionary.
    """
    return {
        "base_path": data.base_path,
        "layout": data.layout,
    }
