"""
load_order_items.py

Reads the raw Olist order_items CSV and writes it out as a staging table
(stg_order_items). This is the table that will become the grain of
Fact_Order_Items in the warehouse -- it's the "many" side that creates
genuine fan-in when joined with orders, products, and sellers.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

from pipeline.instrumentation.run_logger import RunLogger
from pipeline.instrumentation.schema_snapshot import capture_schema

JOB_NAME = "load_order_items"
SOURCE_PATH = "data/olist_order_items_dataset.csv"
TARGET_TABLE = "stg_order_items"


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

        # --- Light transform ---
        clean_df = (
            df
            .withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date")))
            .withColumn("price", col("price").cast("decimal(10,2)"))
            .withColumn("freight_value", col("freight_value").cast("decimal(10,2)"))
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
