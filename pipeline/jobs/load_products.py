"""
load_products.py

Reads the raw Olist products CSV and writes it out as a staging table
(stg_products). Note: this dataset has some nulls in product_category_name
and the dimension columns -- we keep them as-is here (staging should be a
faithful copy of source) and let the Day 3 DQ null checks be the thing that
actually flags data quality issues, rather than silently cleaning them here.
"""

from pyspark.sql import SparkSession

from pipeline.instrumentation.run_logger import RunLogger
from pipeline.instrumentation.schema_snapshot import capture_schema

JOB_NAME = "load_products"
SOURCE_PATH = "data/olist_products_dataset.csv"
TARGET_TABLE = "stg_products"


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

        # --- Write (deliberately minimal transform -- see docstring) ---
        clean_df = df.dropDuplicates(["product_id"])

        capture_schema(clean_df, TARGET_TABLE)

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
