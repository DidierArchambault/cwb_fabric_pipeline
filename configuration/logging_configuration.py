from dataclasses import dataclass


@dataclass
class LoggingConfiguration:
    """
    Configuration for the logging stage of the pipeline.
    """
    main_logging_path: str



def from_dict(data: dict) -> "LoggingConfiguration":
    """
    Create a LoggingConfiguration instance from a dictionary.
    """
    return LoggingConfiguration(
        main_logging_path=data.get("main_logging_path", ""),
    )


def to_dict(data: LoggingConfiguration) -> dict:
    """
    Convert the LoggingConfiguration instance to a dictionary.
    """
    return {
        "main_logging_path": data.main_logging_path,

    }
