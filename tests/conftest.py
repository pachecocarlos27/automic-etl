"""Pytest configuration and shared fixtures."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from automic_etl.core.config import Settings, StorageProvider


@pytest.fixture
def temp_dir() -> Path:
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_df() -> pl.DataFrame:
    """Create a sample DataFrame for testing."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com",
                  "david@example.com", "eve@example.com"],
        "age": [25, 30, 35, 40, 45],
        "salary": [50000.0, 60000.0, 75000.0, 85000.0, 95000.0],
    })


@pytest.fixture
def sample_df_with_nulls() -> pl.DataFrame:
    """Create a sample DataFrame with null values."""
    return pl.DataFrame({
        "id": [1, 2, None, 4, 5],
        "name": ["Alice", None, "Charlie", "David", "Eve"],
        "email": ["alice@example.com", "bob@example.com", None, "david@example.com", None],
        "age": [25, None, 35, 40, 45],
        "salary": [50000.0, 60000.0, None, 85000.0, 95000.0],
    })


@pytest.fixture
def sample_df_duplicates() -> pl.DataFrame:
    """Create a sample DataFrame with duplicate rows."""
    return pl.DataFrame({
        "id": [1, 1, 2, 2, 3],
        "name": ["Alice", "Alice", "Bob", "Bob", "Charlie"],
        "email": ["alice@example.com", "alice@example.com", "bob@example.com",
                  "bob@example.com", "charlie@example.com"],
    })


@pytest.fixture
def test_settings(temp_dir: Path) -> Settings:
    """Create test settings with local storage."""
    return Settings(
        storage__provider="aws",
        storage__aws__bucket="test-bucket",
        storage__aws__region="us-east-1",
        iceberg__warehouse=str(temp_dir / "warehouse"),
        medallion__bronze__path=str(temp_dir / "bronze"),
        medallion__silver__path=str(temp_dir / "silver"),
        medallion__gold__path=str(temp_dir / "gold"),
        llm__api_key="test-key",
    )


@pytest.fixture
def mock_storage_client(monkeypatch):
    """Mock storage client to avoid real cloud calls."""
    mock_client = MagicMock()
    mock_client.write.return_value = True
    mock_client.read.return_value = pl.DataFrame({"test": [1, 2, 3]})

    def mock_get_storage_client(*args, **kwargs):
        return mock_client

    return mock_client


@pytest.fixture
def mock_llm_client(monkeypatch):
    """Mock LLM client for testing."""
    from automic_etl.llm.client import LLMResponse

    mock_client = MagicMock()
    mock_client.complete.return_value = LLMResponse(
        content='{"test": "response"}',
        model="test-model",
        tokens_used=100,
        finish_reason="stop",
    )

    return mock_client


@pytest.fixture
def mock_database_connector(monkeypatch):
    """Mock database connector."""
    mock_conn = MagicMock()
    mock_conn.test_connection.return_value = True
    mock_conn.execute.return_value = pl.DataFrame({"id": [1, 2, 3]})
    mock_conn.execute_batch.return_value = [
        pl.DataFrame({"id": [1, 2]}),
        pl.DataFrame({"id": [3, 4]}),
    ]

    return mock_conn


@pytest.fixture
def disable_external_calls(monkeypatch):
    """Disable external calls during tests."""
    def no_op(*args, **kwargs):
        raise RuntimeError("External calls not allowed in tests. Use mocks instead.")

    monkeypatch.setattr("boto3.client", no_op)
    monkeypatch.setattr("anthropic.Anthropic", no_op)
    monkeypatch.setattr("openai.OpenAI", no_op)


@pytest.fixture(autouse=True)
def setup_logging(caplog):
    """Configure logging for tests."""
    import logging
    caplog.set_level(logging.DEBUG)
    return caplog
