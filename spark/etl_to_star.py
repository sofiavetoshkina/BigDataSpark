from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, monotonically_increasing_id, to_date, dayofmonth, month, year, quarter
)

spark = SparkSession.builder \
    .appName("ETL: Raw -> Star Schema (PostgreSQL)") \
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

print("Читаем сырые данные mock_data из PostgreSQL...")
raw = spark.read.jdbc(PG_URL, "mock_data", properties=PG_PROPS)
raw = raw.cache()
print(f"Строк в mock_data: {raw.count()}")

# --- Измерения ---

dim_customer = raw.select(
    "customer_first_name", "customer_last_name",
    col("customer_age").cast("int").alias("customer_age"),
    "customer_email", "customer_country", "customer_postal_code",
    "customer_pet_type", "customer_pet_name", "customer_pet_breed",
    "pet_category"
).dropDuplicates(["customer_first_name", "customer_last_name", "customer_email"])
dim_customer = dim_customer.withColumn("customer_id", monotonically_increasing_id())

dim_seller = raw.select(
    "seller_first_name", "seller_last_name", "seller_email",
    "seller_country", "seller_postal_code"
).dropDuplicates(["seller_first_name", "seller_last_name", "seller_email"])
dim_seller = dim_seller.withColumn("seller_id", monotonically_increasing_id())

dim_product = raw.select(
    "product_name", "product_category",
    col("product_price").cast("double").alias("product_price"),
    col("product_quantity").cast("int").alias("product_quantity"),
    col("product_weight").cast("double").alias("product_weight"),
    "product_color", "product_size", "product_brand", "product_material",
    "product_description",
    col("product_rating").cast("double").alias("product_rating"),
    col("product_reviews").cast("int").alias("product_reviews"),
    "product_release_date", "product_expiry_date"
).dropDuplicates(["product_name", "product_category"])
dim_product = dim_product.withColumn("product_id", monotonically_increasing_id())

dim_store = raw.select(
    "store_name", "store_location", "store_city", "store_state",
    "store_country", "store_phone", "store_email"
).dropDuplicates(["store_name", "store_city"])
dim_store = dim_store.withColumn("store_id", monotonically_increasing_id())

dim_supplier = raw.select(
    "supplier_name", "supplier_contact", "supplier_email",
    "supplier_phone", "supplier_address", "supplier_city",
    "supplier_country"
).dropDuplicates(["supplier_name", "supplier_email"])
dim_supplier = dim_supplier.withColumn("supplier_id", monotonically_increasing_id())

dim_date = raw.select(
    to_date(col("sale_date"), "M/d/yyyy").alias("full_date")
).dropDuplicates()
dim_date = dim_date \
    .withColumn("date_id", monotonically_increasing_id()) \
    .withColumn("day", dayofmonth(col("full_date"))) \
    .withColumn("month", month(col("full_date"))) \
    .withColumn("year", year(col("full_date"))) \
    .withColumn("quarter", quarter(col("full_date")))

print("Записываем измерения в PostgreSQL...")
dim_customer.write.jdbc(PG_URL, "dim_customer", mode="overwrite", properties=PG_PROPS)
dim_seller.write.jdbc(PG_URL, "dim_seller", mode="overwrite", properties=PG_PROPS)
dim_product.write.jdbc(PG_URL, "dim_product", mode="overwrite", properties=PG_PROPS)
dim_store.write.jdbc(PG_URL, "dim_store", mode="overwrite", properties=PG_PROPS)
dim_supplier.write.jdbc(PG_URL, "dim_supplier", mode="overwrite", properties=PG_PROPS)
dim_date.write.jdbc(PG_URL, "dim_date", mode="overwrite", properties=PG_PROPS)

# --- Таблица фактов ---
print("Собираем fact_sales...")

dc = spark.read.jdbc(PG_URL, "dim_customer", properties=PG_PROPS).select(
    col("customer_first_name").alias("_c_fn"),
    col("customer_last_name").alias("_c_ln"),
    col("customer_email").alias("_c_em"),
    "customer_id"
)
ds = spark.read.jdbc(PG_URL, "dim_seller", properties=PG_PROPS).select(
    col("seller_first_name").alias("_s_fn"),
    col("seller_last_name").alias("_s_ln"),
    col("seller_email").alias("_s_em"),
    "seller_id"
)
dp = spark.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS).select(
    col("product_name").alias("_p_name"),
    col("product_category").alias("_p_cat"),
    "product_id"
)
dst = spark.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS).select(
    col("store_name").alias("_st_name"),
    col("store_city").alias("_st_city"),
    "store_id"
)
dsu = spark.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS).select(
    col("supplier_name").alias("_su_name"),
    col("supplier_email").alias("_su_em"),
    "supplier_id"
)
dd = spark.read.jdbc(PG_URL, "dim_date", properties=PG_PROPS).select(
    col("full_date").alias("_d_date"),
    "date_id"
)

raw_p = raw.withColumn("_sale_date_parsed", to_date(col("sale_date"), "M/d/yyyy"))

fact = raw_p \
    .join(dc, (raw_p["customer_first_name"] == dc["_c_fn"]) &
              (raw_p["customer_last_name"] == dc["_c_ln"]) &
              (raw_p["customer_email"] == dc["_c_em"]), "left") \
    .join(ds, (raw_p["seller_first_name"] == ds["_s_fn"]) &
              (raw_p["seller_last_name"] == ds["_s_ln"]) &
              (raw_p["seller_email"] == ds["_s_em"]), "left") \
    .join(dp, (raw_p["product_name"] == dp["_p_name"]) &
              (raw_p["product_category"] == dp["_p_cat"]), "left") \
    .join(dst, (raw_p["store_name"] == dst["_st_name"]) &
               (raw_p["store_city"] == dst["_st_city"]), "left") \
    .join(dsu, (raw_p["supplier_name"] == dsu["_su_name"]) &
               (raw_p["supplier_email"] == dsu["_su_em"]), "left") \
    .join(dd, (raw_p["_sale_date_parsed"] == dd["_d_date"]), "left")

fact_sales = fact.select(
    col("customer_id"),
    col("seller_id"),
    col("product_id"),
    col("store_id"),
    col("supplier_id"),
    col("date_id"),
    col("sale_quantity").cast("int").alias("sale_quantity"),
    col("sale_total_price").cast("double").alias("sale_total_price")
)

print(f"Строк в fact_sales: {fact_sales.count()}")
fact_sales.write.jdbc(PG_URL, "fact_sales", mode="overwrite", properties=PG_PROPS)
print("Звезда собрана в PostgreSQL.")
spark.stop()
