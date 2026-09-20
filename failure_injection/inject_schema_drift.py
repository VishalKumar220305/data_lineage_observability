"""
inject_schema_drift.py

FAILURE INJECTION SCENARIO 1: Schema drift.

Simulates a bug that silently drops the 'customer_city' column when
writing stg_customers -- e.g. someone refactored load_customers.py and
accidentally removed a .select() column without noticing. This is exactly
the "a single broken upstream table can silently corrupt every report
downstream of it" scenario the whole project exists to catch.

We bypass the real load_customers.py script and write directly to the
Spark table here, since this script's entire purpose is to CREATE the bug
on demand for the demo -- then dq_runner.py (unmodified) is what proves
the system catches it.

Usage:
    python -m failure_injection.inject_schema_drift
"""

from pyspark.sql import SparkSession

from failure_injection._inject_helpers import log_backdated_run

JOB_NAME = "load_customers"   # SAME job_name as the real job -- this is
                               # what makes freshness/MTTD calculations
                               # treat this as "the last write" to
                               # stg_customers.


def run():
    spark = SparkSession.builder \
        .appName("inject_schema_drift") \
        .enableHiveSupport() \
        .getOrCreate()

    print("=" * 70)
    print("FAILURE INJECTION: Schema drift on stg_customers")
    print("Dropping column: customer_city")
    print("=" * 70)

    df = spark.read.table("stg_customers")
    rows_before = df.count()

    print(f"\nBEFORE: stg_customers columns = {df.columns}")

    broken_df = df.drop("customer_city")

    print(f"AFTER:  stg_customers columns = {broken_df.columns}")

    # Spark refuses to overwrite a table while reading from that same table
    # (it can't guarantee the read finished before the overwrite starts).
    # The standard fix: write to a temp table, then swap it in.
    temp_table = "stg_customers_tmp_injection"
    broken_df.write.mode("overwrite").saveAsTable(temp_table)
    rows_after = spark.read.table(temp_table).count()

    spark.sql("DROP TABLE IF EXISTS stg_customers")
    spark.sql(f"ALTER TABLE {temp_table} RENAME TO stg_customers")

    spark.stop()

    log_backdated_run(
        job_name=JOB_NAME,
        scenario_label="Schema drift scenario -- customer_city column dropped from stg_customers",
        rows_read=rows_before,
        rows_written=rows_after,
    )

    print("\nInjection complete. Now run: python -m data_quality.dq_runner")
    print("Expect stg_customers to show:")
    print("  - FAIL on schema_drift (column removed)")
    print("  - FAIL on null_check for customer_city (column not found)")


if __name__ == "__main__":
    run()
