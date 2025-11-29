"""
Basic Pipeline Example
======================

This example demonstrates how to create a simple ETL pipeline using Automic ETL.
It shows the complete flow from data ingestion through the medallion architecture.
"""

import polars as pl
from datetime import datetime

from automic_etl import Settings, get_settings
from automic_etl.medallion import Lakehouse


def create_sample_data() -> pl.DataFrame:
    """Create sample sales data for demonstration."""
    return pl.DataFrame({
        "order_id": ["ORD001", "ORD002", "ORD003", "ORD004", "ORD005"],
        "customer_name": ["John Doe", "Jane Smith", "Bob Wilson", "Alice Brown", "Charlie Davis"],
        "product": ["Widget A", "Widget B", "Widget A", "Widget C", "Widget B"],
        "quantity": [2, 1, 5, 3, 2],
        "unit_price": [29.99, 49.99, 29.99, 19.99, 49.99],
        "order_date": [
            datetime(2024, 1, 15),
            datetime(2024, 1, 16),
            datetime(2024, 1, 16),
            datetime(2024, 1, 17),
            datetime(2024, 1, 18),
        ],
        "region": ["North", "South", "North", "East", "West"],
    })


def main():
    """Run the basic pipeline example."""
    print("=" * 60)
    print("Automic ETL - Basic Pipeline Example")
    print("=" * 60)

    # Initialize lakehouse
    lakehouse = Lakehouse()
    lakehouse.initialize()

    # Create sample data
    df = create_sample_data()
    print(f"\n1. Created sample data with {len(df)} rows")
    print(df)

    # Ingest to Bronze layer
    print("\n2. Ingesting to Bronze layer...")
    rows = lakehouse.ingest(
        table_name="sales_orders",
        data=df,
        source="example_script",
    )
    print(f"   ✓ Ingested {rows} rows to bronze.sales_orders")

    # Process to Silver layer with transformations
    print("\n3. Processing to Silver layer...")

    def add_total_column(df: pl.DataFrame) -> pl.DataFrame:
        """Add calculated total column."""
        return df.with_columns(
            (pl.col("quantity") * pl.col("unit_price")).alias("total_amount")
        )

    rows = lakehouse.process_to_silver(
        bronze_table="sales_orders",
        silver_table="sales_orders",
        transformations=[add_total_column],
        dedup_columns=["order_id"],
    )
    print(f"   ✓ Processed {rows} rows to silver.sales_orders")

    # Query Silver data
    print("\n4. Querying Silver layer...")
    silver_df = lakehouse.query(
        table="sales_orders",
        layer="silver",
    )
    print(silver_df)

    # Aggregate to Gold layer
    print("\n5. Aggregating to Gold layer...")
    from automic_etl.medallion.gold import AggregationType

    rows = lakehouse.aggregate_to_gold(
        silver_table="sales_orders",
        gold_table="sales_by_region",
        group_by=["region"],
        aggregations={
            "total_orders": [("order_id", AggregationType.COUNT)],
            "total_revenue": [("total_amount", AggregationType.SUM)],
            "avg_order_value": [("total_amount", AggregationType.AVG)],
        },
    )
    print(f"   ✓ Aggregated {rows} rows to gold.sales_by_region")

    # Query Gold data
    print("\n6. Querying Gold layer...")
    gold_df = lakehouse.query(
        table="sales_by_region",
        layer="gold",
    )
    print(gold_df)

    # Show final status
    print("\n7. Lakehouse Status:")
    tables = lakehouse.list_tables()
    for layer, layer_tables in tables.items():
        print(f"   {layer}: {layer_tables}")

    print("\n" + "=" * 60)
    print("Pipeline completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
