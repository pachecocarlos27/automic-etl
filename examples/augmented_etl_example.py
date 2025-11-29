"""
Augmented ETL Example
=====================

This example demonstrates the AugmentedETL class which provides high-level
LLM-powered data processing capabilities including:

- Smart data ingestion with auto-classification
- Natural language transformations
- Automated data profiling
- Entity extraction and enrichment
- Pipeline suggestions
"""

import polars as pl
from datetime import datetime

from automic_etl import Settings, AugmentedETL
from automic_etl.medallion import Lakehouse


def create_sample_data() -> pl.DataFrame:
    """Create sample data with mixed content types."""
    return pl.DataFrame({
        "id": [f"REC{i:04d}" for i in range(1, 11)],
        "description": [
            "Meeting with John Smith from Acme Corp on Jan 15, 2024 to discuss $50,000 contract",
            "Invoice #INV-2024-001 for $12,500 due March 1, 2024",
            "Customer complaint from Jane Doe (jane@email.com) about order #ORD-5678",
            "Call scheduled with Robert Johnson (555-123-4567) for product demo",
            "Quarterly review meeting notes - revenue up 15% vs Q3 2023",
            "Support ticket #TKT-9012: Issue with login, user: emily.w@company.com",
            "Partnership proposal from TechCorp Inc worth $250,000 annually",
            "Employee onboarding: Sarah Davis, start date Feb 1, 2024",
            "Contract renewal for ABC Company, value: $175,000",
            "Event registration: Tech Conference 2024, March 15-17, San Francisco",
        ],
        "category": ["Meeting", "Invoice", "Support", "Sales", "Report",
                    "Support", "Partnership", "HR", "Sales", "Event"],
        "amount": [50000, 12500, None, None, None, None, 250000, None, 175000, 1500],
        "created_at": [datetime.now()] * 10,
    })


def demo_smart_ingest():
    """Demonstrate smart data ingestion."""
    print("\n" + "=" * 60)
    print("Smart Data Ingestion")
    print("=" * 60)

    settings = Settings()
    augmented = AugmentedETL(settings)

    # Create sample data
    df = create_sample_data()
    print(f"\nSample data ({len(df)} rows):")
    print(df.select(["id", "category", "amount"]))

    # Smart ingest with AI features
    print("\nPerforming smart ingestion with AI features...")
    result = augmented.smart_ingest(
        data=df,
        source_name="business_records",
        data_type="auto",  # Auto-detect data type
        extract_entities=True,
        classify_data=True,
        infer_schema=True,
    )

    print(f"\nIngestion Result:")
    print(f"  Table: {result['table_name']}")
    print(f"  Rows: {result['row_count']}")
    print(f"  Classification: {result.get('classification', 'N/A')}")
    print(f"  Sensitivity: {result.get('sensitivity_level', 'N/A')}")
    print(f"  PII Detected: {result.get('contains_pii', 'N/A')}")


def demo_data_analysis():
    """Demonstrate AI-powered data analysis."""
    print("\n" + "=" * 60)
    print("AI-Powered Data Analysis")
    print("=" * 60)

    settings = Settings()
    augmented = AugmentedETL(settings)

    df = create_sample_data()

    # Analyze data with custom questions
    print("\nAnalyzing data with AI...")
    analysis = augmented.analyze_data(
        df=df,
        questions=[
            "What types of business activities are represented?",
            "What is the total monetary value mentioned?",
            "Are there any patterns in the data?",
        ],
    )

    print("\nAnalysis Results:")
    for key, value in analysis.items():
        if isinstance(value, list):
            print(f"\n{key}:")
            for item in value[:3]:
                print(f"  - {item}")
        else:
            print(f"  {key}: {value}")


def demo_data_profiling():
    """Demonstrate data profiling."""
    print("\n" + "=" * 60)
    print("Data Profiling")
    print("=" * 60)

    settings = Settings()
    augmented = AugmentedETL(settings)

    df = create_sample_data()

    # Generate profile
    print("\nGenerating data profile...")
    profile = augmented.generate_profile(df)

    print(f"\nProfile Summary:")
    print(f"  Row Count: {profile.row_count}")
    print(f"  Column Count: {profile.column_count}")
    print(f"  Missing Values: {profile.missing_percentage:.1f}%")
    print(f"  Memory Usage: {profile.memory_usage_mb:.2f} MB")

    print("\n  Column Profiles:")
    for col_name, col_profile in list(profile.column_profiles.items())[:3]:
        print(f"    {col_name}:")
        print(f"      Type: {col_profile['dtype']}")
        print(f"      Non-null: {col_profile['non_null_count']}")
        print(f"      Unique: {col_profile['unique_count']}")


def demo_entity_enrichment():
    """Demonstrate entity extraction enrichment."""
    print("\n" + "=" * 60)
    print("Entity Extraction Enrichment")
    print("=" * 60)

    settings = Settings()
    augmented = AugmentedETL(settings)

    df = create_sample_data()

    # Enrich with entities
    print("\nExtracting entities from description column...")
    enriched_df = augmented.enrich_with_entities(
        df=df,
        text_column="description",
        entity_types=["PERSON", "ORGANIZATION", "MONEY", "DATE", "EMAIL", "PHONE"],
    )

    print(f"\nEnriched DataFrame columns: {enriched_df.columns}")
    print("\nSample enriched data:")
    print(enriched_df.select(["id", "description", "_entities"]).head(3))


def demo_pipeline_suggestion():
    """Demonstrate pipeline suggestions."""
    print("\n" + "=" * 60)
    print("Pipeline Suggestions")
    print("=" * 60)

    settings = Settings()
    augmented = AugmentedETL(settings)

    # Get pipeline suggestions
    print("\nGetting AI pipeline suggestions...")
    suggestion = augmented.suggest_pipeline(
        source_description="CSV files with customer transaction data including names, emails, amounts, and dates",
        target_description="Clean, enriched customer analytics table with aggregated metrics",
        requirements=[
            "Remove duplicates",
            "Extract entities from notes",
            "Classify PII",
            "Aggregate by customer",
        ],
    )

    print("\nSuggested Pipeline:")
    print(f"  Name: {suggestion.get('pipeline_name', 'N/A')}")
    print(f"\n  Stages:")
    for i, stage in enumerate(suggestion.get('stages', []), 1):
        print(f"    {i}. {stage.get('name', 'Unknown')}")
        print(f"       Type: {stage.get('type', 'N/A')}")
        print(f"       Description: {stage.get('description', 'N/A')[:60]}...")

    print(f"\n  Estimated Complexity: {suggestion.get('complexity', 'N/A')}")
    print(f"  Recommendations: {suggestion.get('recommendations', [])[:2]}")


def demo_smart_transform():
    """Demonstrate natural language transformations."""
    print("\n" + "=" * 60)
    print("Natural Language Transformations")
    print("=" * 60)

    settings = Settings()
    augmented = AugmentedETL(settings)

    df = create_sample_data()

    # Transform with natural language
    print("\nApplying transformation: 'Filter to only records with amount > 10000, add a priority column based on amount'")
    transformed_df = augmented.smart_transform(
        df=df,
        instructions="Filter to only records with amount > 10000, add a priority column: HIGH if amount > 100000, MEDIUM otherwise",
        validate=True,
    )

    print(f"\nTransformed DataFrame ({len(transformed_df)} rows):")
    print(transformed_df.select(["id", "category", "amount", "priority"]))


def demo_auto_clean():
    """Demonstrate automatic data cleaning."""
    print("\n" + "=" * 60)
    print("Automatic Data Cleaning")
    print("=" * 60)

    settings = Settings()
    augmented = AugmentedETL(settings)

    # Create dirty data
    dirty_df = pl.DataFrame({
        "id": ["1", "2", "2", "3", None],  # Duplicates and nulls
        "name": ["John", "jane", "JANE", "Bob", "Alice"],  # Inconsistent case
        "email": ["john@email.com", "invalid-email", "jane@email.com", "", "alice@email.com"],
        "amount": ["$1,000", "2000", "3000.50", "N/A", "5000"],  # Inconsistent formats
    })

    print("\nDirty data:")
    print(dirty_df)

    # Auto clean
    print("\nApplying auto-clean...")
    cleaned_df = augmented.auto_clean(df=dirty_df, aggressive=False)

    print("\nCleaned data:")
    print(cleaned_df)


def main():
    """Run all augmented ETL demos."""
    print("=" * 60)
    print("Automic ETL - Augmented ETL Examples")
    print("=" * 60)

    # Run demos
    demo_smart_ingest()
    demo_data_analysis()
    demo_data_profiling()
    demo_entity_enrichment()
    demo_pipeline_suggestion()
    demo_smart_transform()
    demo_auto_clean()

    print("\n" + "=" * 60)
    print("All Augmented ETL demos completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
