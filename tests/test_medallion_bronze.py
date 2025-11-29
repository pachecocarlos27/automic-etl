"""Tests for Bronze layer."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from automic_etl.medallion.bronze import BronzeLayer


@pytest.fixture
def bronze_layer(test_settings):
    """Create a BronzeLayer instance for testing."""
    with patch("automic_etl.medallion.bronze.IcebergTableManager"):
        return BronzeLayer(test_settings)


class TestBronzeLayerIngestion:
    """Test Bronze layer data ingestion."""

    def test_ingest_dataframe_success(self, bronze_layer, sample_df):
        """Successfully ingest a DataFrame."""
        with patch.object(bronze_layer.table_manager, "write_table"):
            rows = bronze_layer.ingest(
                table_name="users",
                df=sample_df,
                source="csv_import",
            )

            assert rows == len(sample_df)
            bronze_layer.table_manager.write_table.assert_called_once()

    def test_ingest_empty_dataframe(self, bronze_layer):
        """Ingesting empty DataFrame should return 0."""
        empty_df = pl.DataFrame()
        rows = bronze_layer.ingest(
            table_name="users",
            df=empty_df,
            source="csv_import",
        )

        assert rows == 0

    def test_ingest_with_metadata(self, bronze_layer, sample_df):
        """Ingest with additional metadata."""
        with patch.object(bronze_layer.table_manager, "write_table"):
            bronze_layer.ingest(
                table_name="users",
                df=sample_df,
                source="database",
                source_file="users.csv",
                batch_id="batch_001",
                additional_metadata={"environment": "production"},
            )

            bronze_layer.table_manager.write_table.assert_called_once()

    def test_ingest_metadata_columns_added(self, bronze_layer, sample_df):
        """Verify metadata columns are added to ingested data."""
        with patch.object(bronze_layer.table_manager, "write_table") as mock_write:
            bronze_layer.ingest(
                table_name="users",
                df=sample_df,
                source="api",
            )

            # Get the dataframe that was passed to write_table
            call_args = mock_write.call_args
            written_df = call_args[1]["df"] if call_args else None

            if written_df is not None:
                # Check that metadata columns were added
                assert "_ingestion_time" in written_df.columns
                assert "_source" in written_df.columns
                assert "_ingestion_date" in written_df.columns

    def test_ingest_invalid_table_name(self, bronze_layer, sample_df):
        """Invalid table name should raise error."""
        with pytest.raises(Exception):
            bronze_layer.ingest(
                table_name="123invalid",  # Starts with number
                df=sample_df,
                source="csv_import",
            )


class TestBronzeLayerSemiStructured:
    """Test semi-structured data ingestion."""

    def test_ingest_semi_structured_success(self, bronze_layer):
        """Successfully ingest semi-structured data."""
        with patch.object(bronze_layer.table_manager, "write_table"):
            json_data = {"id": 1, "name": "test", "nested": {"key": "value"}}
            rows = bronze_layer.ingest_semi_structured(
                table_name="json_data",
                data=json_data,
                source="api",
            )

            assert rows > 0
            bronze_layer.table_manager.write_table.assert_called_once()

    def test_ingest_semi_structured_with_schema_inference(self, bronze_layer):
        """Semi-structured data should infer schema."""
        with patch.object(bronze_layer.table_manager, "write_table"):
            json_data = [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
            bronze_layer.ingest_semi_structured(
                table_name="json_array",
                data=json_data,
                source="api",
            )

            bronze_layer.table_manager.write_table.assert_called_once()


class TestBronzeLayerUnstructured:
    """Test unstructured data ingestion."""

    def test_ingest_unstructured_pdf(self, bronze_layer):
        """Successfully ingest unstructured PDF data."""
        pdf_content = b"PDF content"
        with patch("automic_etl.medallion.bronze.extract_text_from_pdf") as mock_extract:
            mock_extract.return_value = "Extracted text from PDF"
            with patch.object(bronze_layer.table_manager, "write_table"):
                rows = bronze_layer.ingest_unstructured(
                    table_name="documents",
                    content=pdf_content,
                    source="document_upload",
                    content_type="application/pdf",
                )

                assert rows > 0

    def test_ingest_unstructured_text(self, bronze_layer):
        """Successfully ingest unstructured text data."""
        text_content = "This is raw text content"
        with patch.object(bronze_layer.table_manager, "write_table"):
            rows = bronze_layer.ingest_unstructured(
                table_name="text_data",
                content=text_content,
                source="text_import",
                content_type="text/plain",
            )

            assert rows > 0


class TestBronzeLayerMetadata:
    """Test metadata handling in Bronze layer."""

    def test_metadata_columns_schema(self, bronze_layer):
        """Verify all metadata columns are defined."""
        expected_columns = {
            "_ingestion_time",
            "_source",
            "_source_file",
            "_batch_id",
            "_ingestion_date",
        }

        assert expected_columns.issubset(set(BronzeLayer.METADATA_COLUMNS))

    def test_metadata_preservation(self, bronze_layer, sample_df):
        """Metadata should be preserved in the table."""
        with patch.object(bronze_layer.table_manager, "write_table") as mock_write:
            bronze_layer.ingest(
                table_name="users",
                df=sample_df,
                source="test_source",
                source_file="test.csv",
            )

            call_args = mock_write.call_args
            written_df = call_args[1]["df"] if call_args else None

            if written_df is not None:
                # Verify source is preserved
                assert written_df["_source"].unique()[0] == "test_source"


class TestBronzeLayerPartitioning:
    """Test partitioning in Bronze layer."""

    def test_ingestion_date_partition(self, bronze_layer, sample_df):
        """Data should be partitioned by ingestion date."""
        with patch.object(bronze_layer.table_manager, "write_table") as mock_write:
            bronze_layer.ingest(
                table_name="users",
                df=sample_df,
                source="test",
            )

            # Verify partitioning parameters were passed
            call_kwargs = mock_write.call_args[1] if mock_write.call_args else {}
            assert "partition_cols" in call_kwargs or "_ingestion_date" in str(call_kwargs)


class TestBronzeLayerErrors:
    """Test error handling in Bronze layer."""

    def test_ingest_write_error(self, bronze_layer, sample_df):
        """Should handle write errors gracefully."""
        with patch.object(
            bronze_layer.table_manager,
            "write_table",
            side_effect=Exception("Write failed"),
        ):
            with pytest.raises(Exception):
                bronze_layer.ingest(
                    table_name="users",
                    df=sample_df,
                    source="test",
                )

    def test_ingest_with_missing_source(self, bronze_layer, sample_df):
        """Missing source identifier should raise error."""
        with patch.object(bronze_layer.table_manager, "write_table"):
            with pytest.raises(Exception):
                bronze_layer.ingest(
                    table_name="users",
                    df=sample_df,
                    source="",  # Empty source
                )
