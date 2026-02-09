from dataclasses import dataclass


@dataclass
class TechnicalsConfiguration:
    """
    Configuration for the bronze stage of the pipeline.
    """
    Row_Hash_ID: bool
    Ingestion_Run_ID: bool
    Schema_ID: bool
    Ingest_Timestamp: bool



def from_dict(data: dict) -> "TechnicalsConfiguration":
    """
    Create a TechnicalsConfiguration instance from a dictionary.
    """
    return TechnicalsConfiguration(
        Row_Hash_ID=data.get("Row_Hash_ID", False),
        Ingestion_Run_ID=data.get("Ingestion_Run_ID", False),
        Schema_ID=data.get("Schema_ID", False),
        Ingest_Timestamp=data.get("Ingest_Timestamp", False),

    )


def to_dict(data: TechnicalsConfiguration) -> dict:
    """
    Convert the TechnicalsConfiguration instance to a dictionary.
    """
    return {
        "Row_Hash_ID": data.Row_Hash_ID,
        "Ingestion_Run_ID": data.Ingestion_Run_ID,
        "Schema_ID": data.Schema_ID,
        "Ingest_Timestamp": data.Ingest_Timestamp,
    }
