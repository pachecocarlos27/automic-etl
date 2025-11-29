"""PDF document connector."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from automic_etl.connectors.base import (
    ConnectorConfig,
    ConnectorType,
    ExtractionResult,
    UnstructuredConnector,
)
from automic_etl.core.exceptions import ExtractionError


@dataclass
class PDFConfig(ConnectorConfig):
    """PDF connector configuration."""

    path: str = ""
    extract_tables: bool = True
    extract_images: bool = False
    ocr_enabled: bool = True
    ocr_language: str = "eng"
    dpi: int = 300

    def __post_init__(self) -> None:
        self.connector_type = ConnectorType.UNSTRUCTURED


class PDFConnector(UnstructuredConnector):
    """PDF document connector using unstructured library."""

    def __init__(self, config: PDFConfig) -> None:
        super().__init__(config)
        self.pdf_config = config

    def connect(self) -> None:
        """Verify path exists."""
        self._connected = True
        self.logger.info("PDF connector ready", path=self.pdf_config.path)

    def disconnect(self) -> None:
        """No persistent connection to close."""
        self._connected = False

    def test_connection(self) -> bool:
        """Test if path is accessible."""
        path = Path(self.pdf_config.path)
        return path.exists() or path.parent.exists()

    def extract(
        self,
        query: str | None = None,
        path: str | None = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract content from PDF."""
        file_path = path or self.pdf_config.path

        try:
            content = self.extract_content(file_path)
            metadata = self.extract_metadata(file_path)

            # Create structured output
            data = {
                "file_path": [str(file_path)],
                "text": [content.get("text", "")],
                "page_count": [content.get("page_count", 0)],
                "has_tables": [bool(content.get("tables"))],
                "table_count": [len(content.get("tables", []))],
            }

            # Add metadata columns
            for key, value in metadata.items():
                if isinstance(value, (str, int, float, bool)):
                    data[f"meta_{key}"] = [value]

            df = pl.DataFrame(data)

            return ExtractionResult(
                data=df,
                row_count=1,
                metadata={
                    "file": str(file_path),
                    "page_count": content.get("page_count", 0),
                    "tables_extracted": len(content.get("tables", [])),
                    **metadata,
                },
            )
        except Exception as e:
            raise ExtractionError(
                f"Failed to extract PDF: {str(e)}",
                source=str(file_path),
            )

    def extract_content(self, source: str | bytes) -> dict[str, Any]:
        """Extract content from PDF source."""
        try:
            from unstructured.partition.pdf import partition_pdf
        except ImportError:
            # Fallback to basic extraction
            return self._basic_extraction(source)

        try:
            elements = partition_pdf(
                filename=source if isinstance(source, str) else None,
                file=source if isinstance(source, bytes) else None,
                strategy="auto",
                extract_images_in_pdf=self.pdf_config.extract_images,
                infer_table_structure=self.pdf_config.extract_tables,
                languages=[self.pdf_config.ocr_language] if self.pdf_config.ocr_enabled else None,
            )

            # Organize content by type
            text_parts = []
            tables = []
            page_count = 0

            for element in elements:
                if hasattr(element, "metadata") and hasattr(element.metadata, "page_number"):
                    page_count = max(page_count, element.metadata.page_number or 0)

                element_type = type(element).__name__

                if element_type == "Table":
                    if hasattr(element, "metadata") and hasattr(element.metadata, "text_as_html"):
                        tables.append(element.metadata.text_as_html)
                    else:
                        tables.append(str(element))
                else:
                    text_parts.append(str(element))

            return {
                "text": "\n\n".join(text_parts),
                "tables": tables,
                "page_count": page_count,
                "element_count": len(elements),
            }

        except Exception as e:
            self.logger.warning(f"Unstructured extraction failed, using fallback: {e}")
            return self._basic_extraction(source)

    def _basic_extraction(self, source: str | bytes) -> dict[str, Any]:
        """Basic PDF extraction without unstructured library."""
        try:
            import pypdf

            if isinstance(source, str):
                reader = pypdf.PdfReader(source)
            else:
                from io import BytesIO
                reader = pypdf.PdfReader(BytesIO(source))

            text_parts = []
            for page in reader.pages:
                text_parts.append(page.extract_text() or "")

            return {
                "text": "\n\n".join(text_parts),
                "tables": [],
                "page_count": len(reader.pages),
            }
        except ImportError:
            return {
                "text": "",
                "tables": [],
                "page_count": 0,
                "error": "pypdf not installed",
            }

    def extract_metadata(self, source: str | bytes) -> dict[str, Any]:
        """Extract metadata from PDF source."""
        try:
            import pypdf

            if isinstance(source, str):
                reader = pypdf.PdfReader(source)
            else:
                from io import BytesIO
                reader = pypdf.PdfReader(BytesIO(source))

            info = reader.metadata or {}

            return {
                "title": info.get("/Title", ""),
                "author": info.get("/Author", ""),
                "subject": info.get("/Subject", ""),
                "creator": info.get("/Creator", ""),
                "producer": info.get("/Producer", ""),
                "creation_date": str(info.get("/CreationDate", "")),
                "modification_date": str(info.get("/ModDate", "")),
                "page_count": len(reader.pages),
            }
        except Exception:
            return {}

    def extract_tables(self, path: str | None = None) -> list[pl.DataFrame]:
        """Extract tables from PDF as DataFrames."""
        file_path = path or self.pdf_config.path

        try:
            import camelot
            tables = camelot.read_pdf(str(file_path), pages="all")
            return [pl.from_pandas(table.df) for table in tables]
        except ImportError:
            self.logger.warning("camelot-py not installed, table extraction unavailable")
            return []
        except Exception as e:
            self.logger.warning(f"Table extraction failed: {e}")
            return []

    def chunk_text(
        self,
        text: str,
        chunk_size: int = 1000,
        overlap: int = 200,
    ) -> list[str]:
        """Split text into overlapping chunks for LLM processing."""
        if len(text) <= chunk_size:
            return [text]

        chunks = []
        start = 0

        while start < len(text):
            end = start + chunk_size

            # Try to break at sentence boundary
            if end < len(text):
                # Look for sentence ending
                for delim in [". ", ".\n", "? ", "! "]:
                    last_delim = text.rfind(delim, start, end)
                    if last_delim > start:
                        end = last_delim + 1
                        break

            chunks.append(text[start:end].strip())
            start = end - overlap

        return chunks
