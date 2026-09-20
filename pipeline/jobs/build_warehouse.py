"""
build_warehouse.py

Reads all 5 staging tables and builds the small star schema:
    Dim_Customer, Dim_Product, Dim_Seller, Dim_Date, Fact_Order_Items

This is the job with real fan-in (4 staging tables feed Fact_Order_Items)
and it's what will demonstrate fan-out later, once dashboards/reports read
from these warehouse tables downstream -- exactly the graph shape we
wanted the lineage view to show.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, date_format, to_date
)

from pipeline.instrumentation.run_logger import RunLogger
from pipeline.instrumentation.schema_snapshot import capture_schema

JOB_NAME = "build_warehouse"


def run():
    spark = SparkSession.builder \
        .appName(JOB_NAME) \
        .enableHiveSupport() \
        .getOrCreate()

    logger = RunLogger(job_name=JOB_NAME)
    logger.start()

    try:
        # --- Read all 5 staging tables (this is where fan-in comes from) ---
        customers = spark.read.table("stg_customers")
        orders = spark.read.table("stg_orders")
        order_items = spark.read.table("stg_order_items")
        products = spark.read.table("stg_products")
        sellers = spark.read.table("stg_sellers")

        total_rows_read = (
            customers.count() + orders.count() + order_items.count()
            + products.count() + sellers.count()
        )

        # --- Dim_Customer ---
        dim_customer = customers.select(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_city"),
            col("customer_state"),
            col("customer_zip_code_prefix"),
        )
        capture_schema(dim_customer, "Dim_Customer")
        dim_customer.write.mode("overwrite").saveAsTable("Dim_Customer")

        # --- Dim_Product ---
        dim_product = products.select(
            col("product_id"),
            col("product_category_name"),
            col("product_weight_g"),
            col("product_length_cm"),
            col("product_height_cm"),
            col("product_width_cm"),
        )
        capture_schema(dim_product, "Dim_Product")
        dim_product.write.mode("overwrite").saveAsTable("Dim_Product")

        # --- Dim_Seller ---
        dim_seller = sellers.select(
            col("seller_id"),
            col("seller_city"),
            col("seller_state"),
            col("seller_zip_code_prefix"),
        )
        capture_schema(dim_seller, "Dim_Seller")
        dim_seller.write.mode("overwrite").saveAsTable("Dim_Seller")

        # --- Dim_Date (built from distinct order purchase DAYS, not exact
        # timestamps -- a date dimension is grained by day) ---
        dim_date = (
            orders
            .select(to_date(col("order_purchase_timestamp")).alias("full_date"))
            .filter(col("full_date").isNotNull())
            .dropDuplicates(["full_date"])
            .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
            .withColumn("year", year(col("full_date")))
            .withColumn("month", month(col("full_date")))
            .withColumn("day", dayofmonth(col("full_date")))
        )
        capture_schema(dim_date, "Dim_Date")
        dim_date.write.mode("overwrite").saveAsTable("Dim_Date")

        # --- Fact_Order_Items (the grain: one row per item per order) ---
        # Joined to orders for customer_id + status, then to Dim_Date by
        # matching the ORDER's purchase day (not shipping_limit_date, which
        # is a different, unrelated timestamp on the item itself).
        orders_with_date_key = orders.select(
            "order_id", "customer_id", "order_status",
            date_format(to_date(col("order_purchase_timestamp")), "yyyyMMdd")
                .cast("int").alias("date_key"),
        )

        fact_order_items = (
            order_items
            .join(orders_with_date_key, on="order_id", how="inner")
            .select(
                col("order_id"),
                col("order_item_id"),
                col("product_id"),
                col("seller_id"),
                col("customer_id"),
                col("date_key"),
                col("price"),
                col("freight_value"),
                col("order_status"),
            )
        )
        capture_schema(fact_order_items, "Fact_Order_Items")
        fact_order_items.write.mode("overwrite").saveAsTable("Fact_Order_Items")

        total_rows_written = (
            dim_customer.count() + dim_product.count() + dim_seller.count()
            + dim_date.count() + fact_order_items.count()
        )

        logger.finish(status="success", rows_read=total_rows_read,
                       rows_written=total_rows_written)
        print(f"[{JOB_NAME}] Done. Built Dim_Customer, Dim_Product, Dim_Seller, "
              f"Dim_Date, Fact_Order_Items")

    except Exception as e:
        logger.finish(status="failed", error_message=str(e))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
