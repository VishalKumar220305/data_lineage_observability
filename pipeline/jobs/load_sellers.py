"""
load_sellers.py

Reads the raw Olist sellers CSV and writes it out as a staging table
(stg_sellers) in Spark's embedded Hive warehouse.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower

from pipeline.instrumentation.run_logger import RunLogger
from pipeline.instrumentation.schema_snapshot import capture_schema

JOB_NAME = "load_sellers"
SOURCE_PATH = "data/olist_sellers_dataset.csv"
TARGET_TABLE = "stg_sellers"


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
            .withColumn("seller_city", trim(lower(col("seller_city"))))
            .withColumn("seller_state", trim(col("seller_state")))
            .dropDuplicates(["seller_id"])
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
