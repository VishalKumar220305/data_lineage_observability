"""
load_orders.py

Reads the raw Olist orders CSV and writes it out as a staging table
(stg_orders) in Spark's embedded Hive warehouse.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

from pipeline.instrumentation.run_logger import RunLogger
from pipeline.instrumentation.schema_snapshot import capture_schema

JOB_NAME = "load_orders"
SOURCE_PATH = "data/olist_orders_dataset.csv"
TARGET_TABLE = "stg_orders"


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

        # --- Light transform: proper timestamp types, drop exact dupes ---
        clean_df = (
            df
            .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))
            .withColumn("order_approved_at", to_timestamp(col("order_approved_at")))
            .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))
            .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date")))
            .dropDuplicates(["order_id"])
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
