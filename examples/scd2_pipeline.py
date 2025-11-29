"""
SCD Type 2 Pipeline Example
===========================

This example demonstrates how to use Slowly Changing Dimension Type 2
to track historical changes in dimension tables.

SCD2 maintains full history by:
- Creating new rows when records change
- Tracking effective date ranges
- Keeping is_current flag for easy querying
"""

import polars as pl
from datetime import datetime, timedelta

from automic_etl import Settings
from automic_etl.medallion import Lakehouse, SCDType2Manager


def create_initial_customer_data() -> pl.DataFrame:
    """Create initial customer dimension data."""
    return pl.DataFrame({
        "customer_id": ["C001", "C002", "C003"],
        "customer_name": ["John Doe", "Jane Smith", "Bob Wilson"],
        "email": ["john@email.com", "jane@email.com", "bob@email.com"],
        "tier": ["Gold", "Silver", "Bronze"],
        "city": ["New York", "Los Angeles", "Chicago"],
    })


def create_updated_customer_data() -> pl.DataFrame:
    """Create updated customer data (simulating changes)."""
    return pl.DataFrame({
        "customer_id": ["C001", "C002", "C003", "C004"],
        "customer_name": ["John Doe", "Jane Smith-Johnson", "Bob Wilson"],  # C002 name changed
        "email": ["john.doe@newemail.com", "jane@email.com", "bob@email.com", "alice@email.com"],  # C001 email changed, C004 new
        "tier": ["Platinum", "Silver", "Gold"],  # C001 upgraded, C003 upgraded
        "city": ["New York", "San Francisco", "Chicago", "Houston"],  # C002 moved
    })


def main():
    """Run the SCD Type 2 example."""
    print("=" * 60)
    print("Automic ETL - SCD Type 2 Example")
    print("=" * 60)

    settings = Settings()
    lakehouse = Lakehouse(settings)
    lakehouse.initialize()
    scd2 = SCDType2Manager(settings)

    # Step 1: Initial Load
    print("\n1. INITIAL LOAD")
    print("-" * 40)

    initial_df = create_initial_customer_data()
    print("Initial customer data:")
    print(initial_df)

    # Apply SCD2 - this will be an initial load
    result = scd2.apply_scd2(
        source_df=initial_df,
        table_name="dim_customers",
        business_keys=["customer_id"],
        namespace="silver",
        effective_date=datetime(2024, 1, 1),
    )

    print(f"\nInitial load result: {result}")

    # View the SCD2 table
    print("\nSCD2 Table after initial load:")
    current_df = scd2.get_current_records("dim_customers")
    print(current_df.select([
        "customer_id", "customer_name", "tier",
        "_scd_effective_from", "_scd_is_current", "_scd_version"
    ]))

    # Step 2: Apply Updates
    print("\n\n2. APPLY UPDATES")
    print("-" * 40)

    updated_df = create_updated_customer_data()
    print("Updated customer data:")
    print(updated_df)

    # Apply changes with a new effective date (simulating a month later)
    result = scd2.apply_scd2(
        source_df=updated_df,
        table_name="dim_customers",
        business_keys=["customer_id"],
        namespace="silver",
        effective_date=datetime(2024, 2, 1),
    )

    print(f"\nUpdate result: {result}")

    # View all records (including history)
    print("\nSCD2 Table after updates (all records):")
    all_records = scd2.table_manager.read("silver", "dim_customers")
    print(all_records.select([
        "customer_id", "customer_name", "tier", "city",
        "_scd_effective_from", "_scd_effective_to", "_scd_is_current", "_scd_version"
    ]).sort(["customer_id", "_scd_version"]))

    # Step 3: Query Current State
    print("\n\n3. QUERY CURRENT STATE")
    print("-" * 40)

    current = scd2.get_current_records("dim_customers")
    print("Current customers (is_current = true):")
    print(current.select([
        "customer_id", "customer_name", "tier", "city", "_scd_version"
    ]))

    # Step 4: Point-in-Time Query
    print("\n\n4. POINT-IN-TIME QUERY")
    print("-" * 40)

    # What was C001's data on Jan 15, 2024?
    print("Customer C001 on Jan 15, 2024:")
    historical = scd2.get_record_at_time(
        table_name="dim_customers",
        business_key_values={"customer_id": "C001"},
        as_of=datetime(2024, 1, 15),
    )
    if historical is not None:
        print(historical.select(["customer_id", "customer_name", "email", "tier"]))

    # What is C001's data on Feb 15, 2024?
    print("\nCustomer C001 on Feb 15, 2024:")
    current_state = scd2.get_record_at_time(
        table_name="dim_customers",
        business_key_values={"customer_id": "C001"},
        as_of=datetime(2024, 2, 15),
    )
    if current_state is not None:
        print(current_state.select(["customer_id", "customer_name", "email", "tier"]))

    # Step 5: View Complete History
    print("\n\n5. COMPLETE HISTORY FOR C001")
    print("-" * 40)

    history = scd2.get_history(
        table_name="dim_customers",
        business_key_values={"customer_id": "C001"},
    )
    print("All versions for customer C001:")
    print(history.select([
        "customer_name", "email", "tier",
        "_scd_effective_from", "_scd_effective_to", "_scd_version"
    ]))

    # Step 6: Simulate a Delete (Soft Delete)
    print("\n\n6. SOFT DELETE")
    print("-" * 40)

    # Mark C003 as deleted
    delete_df = pl.DataFrame({
        "customer_id": ["C003"],
        "customer_name": ["Bob Wilson"],
        "email": ["bob@email.com"],
        "tier": ["Gold"],
        "city": ["Chicago"],
        "_delete_flag": [True],
    })

    print("Deleting customer C003...")
    result = scd2.merge_scd2(
        source_df=delete_df,
        table_name="dim_customers",
        business_keys=["customer_id"],
        delete_indicator="_delete_flag",
        effective_date=datetime(2024, 3, 1),
    )
    print(f"Delete result: {result}")

    # Verify C003 is no longer current
    print("\nCurrent customers after delete:")
    current = scd2.get_current_records("dim_customers")
    print(current.select(["customer_id", "customer_name", "_scd_is_current"]))

    # But history is preserved
    print("\nC003 history (soft delete preserves history):")
    history = scd2.get_history(
        table_name="dim_customers",
        business_key_values={"customer_id": "C003"},
    )
    print(history.select([
        "customer_name", "tier",
        "_scd_effective_from", "_scd_effective_to", "_scd_is_current"
    ]))

    print("\n" + "=" * 60)
    print("SCD Type 2 Example completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
