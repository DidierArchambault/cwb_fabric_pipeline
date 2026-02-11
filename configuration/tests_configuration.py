from dataclasses import dataclass


@dataclass
class TestsConfiguration:
    """
    Configuration for the tests stage of the pipeline.
    """

    tests_dim_path: str
    tests_fact_path: str
    tests_integrity_path: str
    tests_time_path: str
    tests_relations_path: str
    tests_spatial_path: str
    tests_bronze_path: str
    tests_silver_path: str
    tests_gold_path: str


def from_dict(data: dict) -> "TestsConfiguration":
    """
    Create a TestsConfiguration instance from a dictionary.
    """
    return TestsConfiguration(
        tests_dim_path=data.get("tests_dim_path", ""),
        tests_fact_path=data.get("tests_fact_path", ""),
        tests_integrity_path=data.get("tests_integrity_path", ""),
        tests_spatial_path=data.get("tests_spatial_path", ""),
        tests_time_path=data.get("tests_time_path", ""),
        tests_relations_path=data.get("tests_relations_path", ""),
        tests_bronze_path=data.get("tests_bronze_path", ""),
        tests_silver_path=data.get("tests_silver_path", ""),
        tests_gold_path=data.get("tests_gold_path", ""),
    )


def to_dict(data: TestsConfiguration) -> dict:
    """
    Convert the TestsConfiguration instance to a dictionary.
    """
    return {
        "tests_dim_path": data.tests_dim_path,
        "tests_fact_path": data.tests_fact_path,
        "tests_integrity_path": data.tests_integrity_path,
        "tests_spatial_path": data.tests_spatial_path,
        "tests_time_path": data.tests_time_path,
        "tests_relations_path": data.tests_relations_path,
        "tests_bronze_path": data.tests_bronze_path,
        "tests_silver_path": data.tests_silver_path,
        "tests_gold_path": data.tests_gold_path,
    }
