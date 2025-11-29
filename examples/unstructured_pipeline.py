"""
Unstructured Data Pipeline Example
==================================

This example demonstrates how to process unstructured data (PDFs, documents)
using Automic ETL with LLM augmentation for entity extraction.
"""

from pathlib import Path

from automic_etl import Settings
from automic_etl.medallion import Lakehouse
from automic_etl.connectors.unstructured import PDFConnector, DocumentConnector
from automic_etl.llm import EntityExtractor, SchemaGenerator


def process_pdf_documents(pdf_path: str):
    """Process PDF documents and extract structured data."""
    print("=" * 60)
    print("Processing PDF Documents")
    print("=" * 60)

    settings = Settings()
    lakehouse = Lakehouse(settings)

    # Initialize PDF connector
    from automic_etl.connectors.unstructured.pdf import PDFConfig
    config = PDFConfig(
        name="pdf_reader",
        path=pdf_path,
        extract_tables=True,
        ocr_enabled=True,
    )
    pdf_connector = PDFConnector(config)
    pdf_connector.connect()

    # Extract content
    print(f"\n1. Extracting content from: {pdf_path}")
    result = pdf_connector.extract()

    print(f"   ✓ Extracted {result.row_count} document(s)")
    print(f"   Metadata: {result.metadata}")

    # Ingest raw content to Bronze
    print("\n2. Ingesting to Bronze layer...")
    lakehouse.ingest(
        table_name="documents_raw",
        data=result.data,
        source="pdf_ingestion",
        data_type="structured",
    )

    # Use LLM to extract entities
    print("\n3. Extracting entities with LLM...")
    entity_extractor = EntityExtractor(settings)

    # Get text content from result
    text_content = result.data["text"].to_list()[0] if "text" in result.data.columns else ""

    if text_content:
        entities = entity_extractor.extract(
            text=text_content,
            entity_types=["PERSON", "ORGANIZATION", "DATE", "MONEY", "EMAIL", "PHONE"],
        )

        print(f"   ✓ Extracted {len(entities)} entities:")
        for entity in entities[:10]:
            print(f"      {entity.entity_type}: {entity.value} ({entity.confidence:.2f})")

        # Convert to DataFrame and ingest
        entities_df = entity_extractor.extract_to_dataframe(text_content)
        if not entities_df.is_empty():
            lakehouse.ingest(
                table_name="document_entities",
                data=entities_df,
                source="llm_extraction",
            )
            print("\n   ✓ Ingested entities to bronze.document_entities")

    pdf_connector.disconnect()


def process_with_schema_inference(data_path: str):
    """Process data with LLM-based schema inference."""
    print("\n" + "=" * 60)
    print("Schema Inference with LLM")
    print("=" * 60)

    import polars as pl

    settings = Settings()

    # Read sample data
    print(f"\n1. Reading sample data from: {data_path}")

    # For demo, create sample unstructured JSON
    sample_data = pl.DataFrame({
        "raw_text": [
            "John Smith, CEO of Acme Corp, announced $10M funding on Jan 15, 2024. Contact: john@acme.com",
            "Jane Doe (CTO) reported Q4 revenue of $5.2M. Phone: 555-123-4567",
            "Meeting scheduled with Bob Wilson from TechStart Inc for product demo on March 1st.",
        ],
        "source_file": ["doc1.txt", "doc2.txt", "doc3.txt"],
    })

    print("   Sample data:")
    print(sample_data)

    # Infer schema using LLM
    print("\n2. Inferring schema with LLM...")
    schema_gen = SchemaGenerator(settings)
    schema = schema_gen.infer_schema(
        sample_data,
        context="Business communications containing contact information and financial data",
    )

    print("   ✓ Inferred schema:")
    for col in schema.get("columns", []):
        print(f"      {col['name']}: {col['type']} - {col.get('description', 'N/A')}")

    # Suggest partition strategy
    print("\n3. Suggesting partition strategy...")
    partition_rec = schema_gen.suggest_partition_strategy(sample_data, use_case="analytics")
    print(f"   Partition columns: {partition_rec.get('partition_columns', [])}")
    print(f"   Sort columns: {partition_rec.get('sort_columns', [])}")

    return schema


def main():
    """Run the unstructured pipeline example."""
    print("=" * 60)
    print("Automic ETL - Unstructured Data Pipeline Example")
    print("=" * 60)

    # Note: Update these paths to actual files for testing
    # pdf_path = "path/to/your/document.pdf"
    # process_pdf_documents(pdf_path)

    # Demo schema inference with sample data
    schema = process_with_schema_inference("sample_data")

    print("\n" + "=" * 60)
    print("Unstructured pipeline example completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
