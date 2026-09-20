"""
inject_referential_break.py

FAILURE INJECTION SCENARIO 2: Referential integrity break.

Simulates a bug where some Fact_Order_Items rows end up pointing at a
customer_id that doesn't exist in Dim_Customer -- e.g. a join issue, a
customer that was deleted from Dim_Customer without being cascade-handled,
or bad data from an upstream source system.

Usage:
    python -m failure_injection.inject_referential_break
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, monotonically_increasing_id

from failure_injection._inject_helpers import log_backdated_run

JOB_NAME = "build_warehouse"   # SAME job_name as the real job that
                                 # produces Fact_Order_Items
NUM_ROWS_TO_CORRUPT = 50
FAKE_CUSTOMER_ID_PREFIX = "INVALID_CUSTOMER_ID_DEMO_"


def run():
    spark = SparkSession.builder \
        .appName("inject_referential_break") \
        .enableHiveSupport() \
        .getOrCreate()

    print("=" * 70)
    print("FAILURE INJECTION: Referential integrity break on Fact_Order_Items")
    print(f"Corrupting customer_id on {NUM_ROWS_TO_CORRUPT} rows")
    print("=" * 70)

    df = spark.read.table("Fact_Order_Items")
    total_before = df.count()

    # Pick a small, deterministic sample by its natural key, then split the
    # table into "the sample" (which we corrupt) and "everything else"
    # (which stays untouched) -- using an anti-join keeps this precise
    # rather than approximate.
    sample_keys = df.limit(NUM_ROWS_TO_CORRUPT).select("order_id", "order_item_id")

    rest_df = df.join(sample_keys, on=["order_id", "order_item_id"], how="left_anti")
    corrupted_sample = (
        df.join(sample_keys, on=["order_id", "order_item_id"], how="inner")
          .withColumn("customer_id", concat(lit(FAKE_CUSTOMER_ID_PREFIX), monotonically_increasing_id()))
    )

    final_df = rest_df.unionByName(corrupted_sample)

    print(f"\nRows before: {total_before}")
    print(f"Rows corrupted: {corrupted_sample.count()}")

    # Same self-reference issue as the schema drift script: can't overwrite
    # a table while reading from it. Write to a temp table, then swap it in.
    temp_table = "fact_order_items_tmp_injection"
    final_df.write.mode("overwrite").saveAsTable(temp_table)
    rows_after = spark.read.table(temp_table).count()

    spark.sql("DROP TABLE IF EXISTS Fact_Order_Items")
    spark.sql(f"ALTER TABLE {temp_table} RENAME TO Fact_Order_Items")

    print(f"Rows after: {rows_after} (should match rows before)")

    spark.stop()

    log_backdated_run(
        job_name=JOB_NAME,
        scenario_label=(f"Referential break scenario -- {NUM_ROWS_TO_CORRUPT} "
                         f"Fact_Order_Items rows given a customer_id not in Dim_Customer"),
        rows_read=total_before,
        rows_written=rows_after,
    )

    print("\nInjection complete. Now run: python -m data_quality.dq_runner")
    print("Expect Fact_Order_Items to show:")
    print(f"  - FAIL on referential_integrity for customer_id "
          f"(orphan_key_count={NUM_ROWS_TO_CORRUPT}, since each corrupted "
          f"row got a unique fake ID)")


if __name__ == "__main__":
    run()
