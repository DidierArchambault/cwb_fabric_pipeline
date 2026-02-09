import pytest


@pytest.fixture
def sample_path_data():
    return {
        "path_id": "12345",
        "name": "Sample Path",
        "nodes": [
            {"id": "node1", "type": "start"},
            {"id": "node2", "type": "intermediate"},
            {"id": "node3", "type": "end"},
        ],
        "metadata": {
            "created_by": "tester",
            "created_at": "2024-01-01T12:00:00Z",
        },
    }