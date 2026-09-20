"""
load_customers.py

Reads the raw Olist customers CSV, does light cleaning, and writes it out
as a staging table (stg_customers) in Spark's embedded Hive warehouse.

This is one of the scripts the Day 2 AST parser will scan to auto-detect
lineage -- notice the read pattern (spark.read.csv) and the write pattern
(.saveAsTable), which match what config.yaml's naming_conventions section
told the parser to look for.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower

from pipeline.instrumentation.run_logger import RunLogger
from pipeline.instrumentation.schema_snapshot import capture_schema

JOB_NAME = "load_customers"
SOURCE_PATH = "data/olist_customers_dataset.csv"
TARGET_TABLE = "stg_customers"


def run():
    spark = SparkSession.builder \
        .appName(JOB_NAME) \
        .enableHiveSupport() \
        .getOrCreate()

    logger = RunLogger(job_name=JOB_NAME)
    logger.start()

    try:
        # --- Read ---
        df = spark.read.csv(SOURCE_PATH, header=True, inferSchema=True)
        rows_read = df.count()

        # --- Light transform / cleaning ---
        clean_df = (
            df
            .withColumn("customer_city", trim(lower(col("customer_city"))))
            .withColumn("customer_state", trim(col("customer_state")))
            .dropDuplicates(["customer_id"])
        )

        capture_schema(clean_df, TARGET_TABLE)

        # --- Write ---
        clean_df.write.mode("overwrite").saveAsTable(TARGET_TABLE)
        rows_written = clean_df.count()

        logger.finish(status="success", rows_read=rows_read, rows_written=rows_written)
        print(f"[{JOB_NAME}] Done. rows_read={rows_read} rows_written={rows_written}")

    except Exception as e:
        logger.finish(status="failed", error_message=str(e))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
