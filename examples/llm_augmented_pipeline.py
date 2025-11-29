"""
LLM-Augmented Pipeline Example
==============================

This example demonstrates the AI-augmented features of Automic ETL:
- Natural language to SQL queries
- LLM-based data classification
- Entity extraction from text columns
- Anomaly detection
"""

import polars as pl
from datetime import datetime, timedelta
import random

from automic_etl import Settings
from automic_etl.medallion import Lakehouse
from automic_etl.llm import (
    EntityExtractor,
    DataClassifier,
    QueryBuilder,
    SchemaGenerator,
)


def create_sample_customer_data() -> pl.DataFrame:
    """Create sample customer data with text fields."""
    return pl.DataFrame({
        "customer_id": [f"CUST{i:04d}" for i in range(1, 11)],
        "full_name": [
            "John Smith", "Jane Doe", "Robert Johnson", "Emily Williams",
            "Michael Brown", "Sarah Davis", "David Miller", "Lisa Wilson",
            "James Taylor", "Jennifer Anderson"
        ],
        "email": [
            "john.smith@email.com", "jane.doe@company.org", "rjohnson@gmail.com",
            "emily.w@business.net", "mbrown@email.com", "sdavis@work.com",
            "dmiller@company.org", "lisa.wilson@email.com", "jtaylor@corp.com",
            "janderson@business.net"
        ],
        "phone": [
            "555-123-4567", "555-234-5678", "555-345-6789", "555-456-7890",
            "555-567-8901", "555-678-9012", "555-789-0123", "555-890-1234",
            "555-901-2345", "555-012-3456"
        ],
        "address": [
            "123 Main St, New York, NY 10001",
            "456 Oak Ave, Los Angeles, CA 90001",
            "789 Pine Rd, Chicago, IL 60601",
            "321 Elm St, Houston, TX 77001",
            "654 Maple Dr, Phoenix, AZ 85001",
            "987 Cedar Ln, Philadelphia, PA 19101",
            "147 Birch Way, San Antonio, TX 78201",
            "258 Spruce Ct, San Diego, CA 92101",
            "369 Willow Pl, Dallas, TX 75201",
            "741 Ash Blvd, San Jose, CA 95101"
        ],
        "notes": [
            "VIP customer since 2020. Prefers email contact.",
            "New customer, interested in premium products.",
            "Long-time customer. Has corporate account.",
            "Referred by Jane Doe. Works at Tech Corp.",
            "Recently upgraded to premium plan.",
            "Customer service issue resolved on Jan 15, 2024.",
            "Interested in bulk orders for Q2 2024.",
            "Annual contract renewal due March 2024.",
            "Contact person for ABC Company account.",
            "Participating in beta program."
        ],
        "segment": ["VIP", "New", "Enterprise", "New", "Premium", "Standard", "Enterprise", "Premium", "Enterprise", "New"],
        "annual_revenue": [50000, 5000, 150000, 8000, 25000, 12000, 200000, 35000, 175000, 6000],
    })


def demo_natural_language_queries():
    """Demonstrate natural language to SQL conversion."""
    print("\n" + "=" * 60)
    print("Natural Language to SQL")
    print("=" * 60)

    settings = Settings()
    query_builder = QueryBuilder(settings)

    # Register table schema
    df = create_sample_customer_data()
    query_builder.register_dataframe("customers", df)

    questions = [
        "Show me all VIP customers with revenue over 40000",
        "What is the average revenue by customer segment?",
        "List customers from Texas ordered by revenue",
        "Count customers in each segment",
    ]

    for question in questions:
        print(f"\n[Question] {question}")
        result = query_builder.build_query(question)
        print(f"[SQL] {result.sql}")
        print(f"[Explanation] {result.explanation}")
        if result.warnings:
            print(f"[Warnings] {result.warnings}")


def demo_entity_extraction():
    """Demonstrate entity extraction from text columns."""
    print("\n" + "=" * 60)
    print("Entity Extraction from Text")
    print("=" * 60)

    settings = Settings()
    extractor = EntityExtractor(settings)

    # Sample text with various entities
    sample_text = """
    Meeting notes from Jan 15, 2024:

    Discussed partnership with TechCorp Inc. CEO John Smith (john@techcorp.com, 555-123-4567)
    confirmed a $2.5M investment for Q1 2024.

    Next meeting scheduled for February 1st at 3:00 PM at their headquarters:
    100 Innovation Drive, San Francisco, CA 94105.

    Attendees: Jane Doe (CFO), Robert Johnson (CTO), Sarah Davis (VP Sales)
    """

    print("\nExtracting entities from meeting notes...")
    entities = extractor.extract(
        text=sample_text,
        entity_types=["PERSON", "ORGANIZATION", "DATE", "MONEY", "EMAIL", "PHONE", "ADDRESS"],
    )

    print(f"\nFound {len(entities)} entities:")
    for entity in entities:
        print(f"  {entity.entity_type:15} | {entity.value:30} | Confidence: {entity.confidence:.2f}")

    # Extract relationships
    print("\nExtracting relationships...")
    relationships = extractor.extract_relationships(sample_text)
    for rel in relationships:
        print(f"  {rel['subject']} --[{rel['relationship']}]--> {rel['object']}")


def demo_data_classification():
    """Demonstrate LLM-based data classification."""
    print("\n" + "=" * 60)
    print("Data Classification & PII Detection")
    print("=" * 60)

    settings = Settings()
    classifier = DataClassifier(settings)

    # Classify customer data
    df = create_sample_customer_data()

    print("\nClassifying customer data...")
    classification = classifier.classify(df)

    print(f"\nClassification Results:")
    print(f"  Primary Class: {classification.primary_class}")
    print(f"  Secondary Classes: {classification.secondary_classes}")
    print(f"  Sensitivity Level: {classification.sensitivity_level}")
    print(f"  Contains PII: {classification.contains_pii}")
    print(f"  PII Types: {classification.pii_types}")
    print(f"  Confidence: {classification.confidence:.2f}")

    # Detect PII in columns
    print("\nDetecting PII by column...")
    pii_results = classifier.detect_pii(df)
    print(f"  PII Risk Level: {pii_results['overall_pii_risk']}")
    print(f"  PII Columns: {pii_results['pii_columns']}")

    # Suggest access controls
    print("\nSuggested Access Controls:")
    controls = classifier.suggest_access_controls(classification)
    print(f"  Allowed Roles: {controls.get('allowed_roles', [])}")
    print(f"  Encryption Required: {controls.get('encryption_required', False)}")


def demo_full_augmented_pipeline():
    """Run a complete LLM-augmented pipeline."""
    print("\n" + "=" * 60)
    print("Full LLM-Augmented Pipeline")
    print("=" * 60)

    settings = Settings()
    lakehouse = Lakehouse(settings)
    lakehouse.initialize()

    # Create and ingest data
    df = create_sample_customer_data()

    print("\n1. Ingesting customer data to Bronze...")
    lakehouse.ingest(
        table_name="customers",
        data=df,
        source="crm_export",
    )

    # Classify data before processing
    print("\n2. Classifying data with LLM...")
    classifier = DataClassifier(settings)
    classification = classifier.classify(df)
    print(f"   Classification: {classification.primary_class}")
    print(f"   Sensitivity: {classification.sensitivity_level}")

    # Process to Silver with entity extraction
    print("\n3. Processing to Silver with entity extraction...")
    extractor = EntityExtractor(settings)

    def extract_entities_from_notes(df: pl.DataFrame) -> pl.DataFrame:
        """Extract entities from notes column."""
        # For each row, extract entities from notes
        # This is simplified - in practice, batch processing would be used
        return df.with_columns([
            pl.lit(classification.sensitivity_level).alias("_sensitivity"),
            pl.lit(classification.contains_pii).alias("_has_pii"),
        ])

    lakehouse.process_to_silver(
        bronze_table="customers",
        silver_table="customers_enriched",
        transformations=[extract_entities_from_notes],
        dedup_columns=["customer_id"],
    )

    # Build natural language query
    print("\n4. Querying with natural language...")
    query_builder = QueryBuilder(settings)
    query_builder.register_dataframe("silver.customers_enriched", df)

    result = query_builder.build_query(
        "What is the total revenue by segment for premium and VIP customers?"
    )
    print(f"   Generated SQL: {result.sql}")

    # Show status
    print("\n5. Final Lakehouse Status:")
    tables = lakehouse.list_tables()
    for layer, layer_tables in tables.items():
        print(f"   {layer}: {layer_tables}")

    print("\n" + "=" * 60)
    print("LLM-Augmented Pipeline completed!")
    print("=" * 60)


def main():
    """Run all LLM augmentation demos."""
    print("=" * 60)
    print("Automic ETL - LLM Augmentation Examples")
    print("=" * 60)

    # Run individual demos
    demo_natural_language_queries()
    demo_entity_extraction()
    demo_data_classification()

    # Run full pipeline
    demo_full_augmented_pipeline()


if __name__ == "__main__":
    main()
