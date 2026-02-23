from dataclasses import dataclass


@dataclass
class ObservabilityConfiguration:
    """
    Configuration for the observability aspects of the pipeline.
    """

    runs_logs: str
    events_logs: str
    artifacts_root: str
    log_level: str


def from_dict(data: dict) -> "ObservabilityConfiguration":
    """
    Create an ObservabilityConfiguration instance from a dictionary.
    """
    return ObservabilityConfiguration(
        runs_logs=data.get("runs_logs", ""),
        events_logs=data.get("events_logs", ""),
        artifacts_root=data.get("artifacts_root", ""),
        log_level=data.get("log_level", ""),
    )


def to_dict(data: ObservabilityConfiguration) -> dict:
    """
    Convert the ObservabilityConfiguration instance to a dictionary.
    """
    return {
        "runs_logs": data.runs_logs,
        "events_logs": data.events_logs,
        "artifacts_root": data.artifacts_root,
        "log_level": data.log_level,
    }
