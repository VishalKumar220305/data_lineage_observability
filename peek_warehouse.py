"""
peek_warehouse.py

Since the star-schema tables live in Spark's embedded Hive metastore (not
MySQL), you can't browse them in MySQL Workbench. This script gives you a
quick way to list and preview them instead.

Usage:
    python peek_warehouse.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("peek_warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

print("\nTables currently in the warehouse:")
spark.sql("SHOW TABLES").show(truncate=False)

for table in ["stg_customers", "stg_orders", "stg_order_items", "stg_products",
              "stg_sellers", "Dim_Customer", "Dim_Product", "Dim_Seller",
              "Dim_Date", "Fact_Order_Items"]:
    try:
        df = spark.read.table(table)
        print(f"\n--- {table} ({df.count()} rows) ---")
        df.show(3, truncate=False)
    except Exception:
        print(f"\n--- {table}: not created yet ---")

spark.stop()
