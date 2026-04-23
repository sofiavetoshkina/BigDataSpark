from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count as _count, avg as _avg, concat_ws
)

spark = SparkSession.builder \
    .appName("ETL: Star Schema -> ClickHouse Marts") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

PG_URL = "jdbc:postgresql://postgres:5432/bigdata_lab2"
PG_PROPS = {
    "user": "my_user",
    "password": "12345",
    "driver": "org.postgresql.Driver"
}

CH_URL = "jdbc:clickhouse://clickhouse:8123/default?compress=0"
CH_PROPS = {
    "user": "click",
    "password": "click",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

print("Читаем звезду из PostgreSQL...")
fact_sales = spark.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS)
dim_customer = spark.read.jdbc(PG_URL, "dim_customer", properties=PG_PROPS)
dim_product = spark.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)
dim_store = spark.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS)
dim_supplier = spark.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS)
dim_date = spark.read.jdbc(PG_URL, "dim_date", properties=PG_PROPS)

fact_product = fact_sales.join(dim_product, on="product_id", how="left")
fact_customer = fact_sales.join(dim_customer, on="customer_id", how="left")
fact_date = fact_sales.join(dim_date, on="date_id", how="left")
fact_store = fact_sales.join(dim_store, on="store_id", how="left")
fact_supplier = fact_sales.join(dim_supplier, on="supplier_id", how="left") \
    .join(dim_product, on="product_id", how="left")

# 1. Витрина продаж по продуктам
mart_product_sales = fact_product.groupBy("product_name", "product_category").agg(
    _sum("sale_quantity").alias("total_quantity_sold"),
    _sum("sale_total_price").alias("total_revenue"),
    _avg("product_rating").alias("avg_rating"),
    _sum("product_reviews").alias("total_reviews")
)

# 2. Витрина продаж по клиентам
mart_customer_sales = fact_customer.withColumn(
    "customer_name",
    concat_ws(" ", col("customer_first_name"), col("customer_last_name"))
).groupBy("customer_name", "customer_country").agg(
    _sum("sale_total_price").alias("total_spent"),
    _count("*").alias("num_purchases"),
    _avg("sale_total_price").alias("avg_check")
)

# 3. Витрина продаж по времени
mart_time_sales = fact_date.groupBy("year", "month").agg(
    _sum("sale_total_price").alias("total_sales"),
    _count("*").alias("num_orders"),
    _avg("sale_total_price").alias("avg_order_size")
)

# 4. Витрина продаж по магазинам
mart_store_sales = fact_store.groupBy("store_name", "store_city", "store_country").agg(
    _sum("sale_total_price").alias("total_revenue"),
    _count("*").alias("num_sales"),
    _avg("sale_total_price").alias("avg_check")
)

# 5. Витрина продаж по поставщикам
mart_supplier_sales = fact_supplier.groupBy("supplier_name", "supplier_country").agg(
    _sum("sale_total_price").alias("total_revenue"),
    _avg("product_price").alias("avg_product_price"),
    _count("*").alias("num_products_sold")
)

# 6. Витрина качества продукции
mart_product_quality = fact_product.groupBy("product_name", "product_category").agg(
    _avg("product_rating").alias("avg_rating"),
    _sum("product_reviews").alias("total_reviews"),
    _sum("sale_quantity").alias("total_quantity_sold"),
    _sum("sale_total_price").alias("total_revenue")
)

print("Пишем витрины в ClickHouse...")

mart_product_sales.write.jdbc(CH_URL, "mart_product_sales", mode="overwrite", properties=CH_PROPS)
mart_customer_sales.write.jdbc(CH_URL, "mart_customer_sales", mode="overwrite", properties=CH_PROPS)
mart_time_sales.write.jdbc(CH_URL, "mart_time_sales", mode="overwrite", properties=CH_PROPS)
mart_store_sales.write.jdbc(CH_URL, "mart_store_sales", mode="overwrite", properties=CH_PROPS)
mart_supplier_sales.write.jdbc(CH_URL, "mart_supplier_sales", mode="overwrite", properties=CH_PROPS)
mart_product_quality.write.jdbc(CH_URL, "mart_product_quality", mode="overwrite", properties=CH_PROPS)

print("Все 6 витрин записаны в ClickHouse.")
spark.stop()
