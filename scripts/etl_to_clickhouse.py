from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from etl_reports import (
    build_product_sales,
    build_customer_sales,
    build_monthly_sales,
    build_store_sales,
    build_supplier_sales,
    build_product_quality
)

spark = SparkSession.builder.appName('ETL to ClickHouse').getOrCreate()

pg_url = 'jdbc:postgresql://postgres:5432/lab_db2'
pg_props = {
    'user': 'postgres',
    'password': 'postgres',
    'driver': 'org.postgresql.Driver'
}

ch_url = 'jdbc:clickhouse://clickhouse:8123/reports'
ch_props = {
    'user': 'default',
    'password': '123',
    'driver': 'com.clickhouse.jdbc.ClickHouseDriver'
}

dim_product = spark.read.jdbc(pg_url, 'dwh.dim_product', properties=pg_props)
dim_customer = spark.read.jdbc(pg_url, 'dwh.dim_customer', properties=pg_props)
dim_store = spark.read.jdbc(pg_url, 'dwh.dim_store', properties=pg_props)
dim_supplier = spark.read.jdbc(pg_url, 'dwh.dim_supplier', properties=pg_props)
dim_category = spark.read.jdbc(
    pg_url, 'dwh.dim_product_category', properties=pg_props)
fact_sale = spark.read.jdbc(pg_url, 'dwh.fact_sale', properties=pg_props)

dim_address = spark.read.jdbc(pg_url, 'dwh.dim_address', properties=pg_props)

build_product_sales(dim_product, fact_sale, dim_category).write.jdbc(
    ch_url, 'reports.product_sales', mode='append', properties=ch_props
)
build_customer_sales(dim_customer, fact_sale, dim_address).write.jdbc(
    ch_url, 'customer_sales', mode='append', properties=ch_props
)

build_monthly_sales(fact_sale).write.jdbc(
    ch_url, 'reports.monthly_sales', mode='append', properties=ch_props
)
build_store_sales(dim_store, fact_sale, dim_address).write.jdbc(
    ch_url, 'store_sales', mode='append', properties=ch_props
)
build_supplier_sales(dim_supplier, dim_product, fact_sale, dim_address).write.jdbc(
    ch_url, 'supplier_sales', mode='append', properties=ch_props
)
build_product_quality(dim_product, fact_sale, dim_category).write.jdbc(
    ch_url, 'reports.product_quality', mode='append', properties=ch_props
)


spark.stop()
