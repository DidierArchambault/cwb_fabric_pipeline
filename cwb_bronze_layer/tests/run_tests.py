import pytest
import logging

logger = logging.getLogger(__name__)


def run(pipeline_run_id: str) -> bool:
    logger.info("Starting tests with pytest. Run ID: %s", pipeline_run_id)

    result = pytest.main(
        [
            "-q",
            "tests/QC/business_logics/facts/fact_cs_duration_used_check.py",
            # "--maxfail=2",
            # "-m", "not slow",
        ]
    )

    return result == 0
