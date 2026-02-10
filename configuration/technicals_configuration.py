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
    Claim_SK: str
    Surrogate_Cols: list[str]
    Hash_Cols: list[str]


def from_dict(data: dict) -> "TechnicalsConfiguration":
    """
    Create a TechnicalsConfiguration instance from a dictionary.
    """
    return TechnicalsConfiguration(
        Row_Hash_ID=data.get("Row_Hash_ID", False),
        Ingestion_Run_ID=data.get("Ingestion_Run_ID", False),
        Schema_ID=data.get("Schema_ID", False),
        Ingest_Timestamp=data.get("Ingest_Timestamp", False),
        Claim_SK=data.get("Claim_SK", ""),
        Hash_Cols=data.get("Hash_Cols", []),
        Surrogate_Cols=data.get("Surrogate_Cols", []),
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
        "Claim_SK": data.Claim_SK,
        "Hash_Cols": data.Hash_Cols,
        "Surrogate_Cols": data.Surrogate_Cols,
    }
