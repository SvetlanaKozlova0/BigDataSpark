from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark = SparkSession.builder.appName('ETL Mock to DWH').getOrCreate()

pg_url = 'jdbc:postgresql://postgres:5432/lab_db2'
pg_props = {
    'user': 'postgres',
    'password': 'postgres',
    'driver': 'org.postgresql.Driver'
}

mock_df = spark.read.jdbc(
    url=pg_url, table='mock.mock_data', properties=pg_props)

addr_customer = mock_df.select(
    F.col('customer_country').alias('country'),
    F.col('customer_postal_code').alias('postal_code'),
    F.lit(None).cast('string').alias('city'),
    F.lit(None).cast('string').alias('state'),
    F.lit(None).cast('string').alias('address_line')
).distinct()

addr_seller = mock_df.select(
    F.col('seller_country').alias('country'),
    F.col('seller_postal_code').alias('postal_code'),
    F.lit(None).cast('string').alias('city'),
    F.lit(None).cast('string').alias('state'),
    F.lit(None).cast('string').alias('address_line')
).distinct()

addr_store = mock_df.select(
    F.col('store_country').alias('country'),
    F.lit(None).cast('string').alias('postal_code'),
    F.col('store_city').alias('city'),
    F.col('store_state').alias('state'),
    F.col('store_location').alias('address_line')
).distinct()

addr_supplier = mock_df.select(
    F.col('supplier_country').alias('country'),
    F.lit(None).cast('string').alias('postal_code'),
    F.col('supplier_city').alias('city'),
    F.lit(None).cast('string').alias('state'),
    F.col('supplier_address').alias('address_line')
).distinct()

dim_address = addr_customer.union(addr_seller).union(addr_store).union(addr_supplier).distinct() \
    .filter(F.col('country').isNotNull()) \
    .withColumn('address_id', F.row_number().over(Window.orderBy('country',
                                                                 'postal_code',
                                                                 'city',
                                                                 'state',
                                                                 'address_line')))

dim_address.write.jdbc(url=pg_url, table='dwh.dim_address',
                       mode='append', properties=pg_props)

dim_brand = mock_df.select(
    F.col('product_brand').alias('name')
).distinct().filter(F.col('name').isNotNull()) \
    .withColumn('brand_id', F.row_number().over(Window.orderBy('name')))

dim_brand.write.jdbc(url=pg_url, table='dwh.dim_brand',
                     mode='append', properties=pg_props)

dim_product_category = mock_df.select(
    F.col('product_category').alias('name')).distinct() \
    .filter(F.col('name').isNotNull()) \
    .withColumn('product_category_id', F.row_number().over(Window.orderBy('name')))

dim_product_category.write.jdbc(
    url=pg_url, table='dwh.dim_product_category', mode='append', properties=pg_props)

dim_pet_category = mock_df.select(
    F.col('pet_category').alias('name')).distinct() \
    .filter(F.col('name').isNotNull()) \
    .withColumn('pet_category_id', F.row_number().over(Window.orderBy('name')))

dim_pet_category.write.jdbc(
    url=pg_url, table='dwh.dim_pet_category', mode='append', properties=pg_props)

addr_df = spark.read.jdbc(
    url=pg_url, table='dwh.dim_address', properties=pg_props)

dim_customer = mock_df.select(
    F.col('customer_first_name').alias('first_name'),
    F.col('customer_last_name').alias('last_name'),
    F.col('customer_age').alias('age'),
    F.col('customer_email').alias('email'),
    F.col('customer_country').alias('country'),
    F.col('customer_postal_code').alias('postal_code')
).distinct() \
    .join(
        addr_df.filter(F.col('city').isNull() & F.col(
            'state').isNull() & F.col('address_line').isNull())
    .select(
            F.col('address_id'),
            F.col('country').alias('a_country'),
            F.col('postal_code').alias('a_postal_code')
    ),
        F.col('country').eqNullSafe(F.col('a_country')) &
        F.col('postal_code').eqNullSafe(F.col('a_postal_code')),
        how='left'
).drop('country', 'postal_code', 'a_country', 'a_postal_code') \
    .filter(F.col('email').isNotNull()) \
    .withColumn('customer_id', F.row_number().over(Window.orderBy('email')))

dim_customer.write.jdbc(url=pg_url, table='dwh.dim_customer',
                        mode='append', properties=pg_props)

dim_seller = mock_df.select(
    F.col('seller_first_name').alias('first_name'),
    F.col('seller_last_name').alias('last_name'),
    F.col('seller_email').alias('email'),
    F.col('seller_country').alias('country'),
    F.col('seller_postal_code').alias('postal_code')
).distinct() \
    .join(
        addr_df.filter(F.col('city').isNull() & F.col(
            'state').isNull() & F.col('address_line').isNull())
    .select(
            F.col('address_id'),
            F.col('country').alias('a_country'),
            F.col('postal_code').alias('a_postal_code')
    ),
        F.col('country').eqNullSafe(F.col('a_country')) &
        F.col('postal_code').eqNullSafe(F.col('a_postal_code')),
        how='left'
).drop('country', 'postal_code', 'a_country', 'a_postal_code')\
    .filter(F.col('email').isNotNull()) \
    .withColumn('seller_id', F.row_number().over(Window.orderBy('email')))

dim_seller.write.jdbc(url=pg_url, table='dwh.dim_seller',
                      mode='append', properties=pg_props)

dim_store = mock_df.select(
    F.col('store_name').alias('name'),
    F.col('store_phone').alias('phone'),
    F.col('store_email').alias('email'),
    F.col('store_country').alias('country'),
    F.col('store_city').alias('city'),
    F.col('store_state').alias('state'),
    F.col('store_location').alias('address_line')
).distinct() \
    .join(
        addr_df.filter(F.col('postal_code').isNull())
               .select(
                   F.col('address_id'),
                   F.col('country').alias('a_country'),
                   F.col('city').alias('a_city'),
                   F.col('state').alias('a_state'),
                   F.col('address_line').alias('a_address_line')
        ),
        F.col('country').eqNullSafe(F.col('a_country')) &
        F.col('city').eqNullSafe(F.col('a_city')) &
        F.col('state').eqNullSafe(F.col('a_state')) &
        F.col('address_line').eqNullSafe(F.col('a_address_line')),
        how='left'
).drop('country', 'city', 'state', 'address_line', 'a_country', 'a_city', 'a_state', 'a_address_line') \
    .filter(F.col('email').isNotNull()) \
    .withColumn('store_id', F.row_number().over(Window.orderBy('email')))

dim_store.write.jdbc(url=pg_url, table='dwh.dim_store',
                     mode='append', properties=pg_props)

dim_supplier = mock_df.select(
    F.col('supplier_name').alias('name'),
    F.col('supplier_contact').alias('contact'),
    F.col('supplier_email').alias('email'),
    F.col('supplier_phone').alias('phone'),
    F.col('supplier_country').alias('country'),
    F.col('supplier_city').alias('city'),
    F.col('supplier_address').alias('address_line')
).distinct() \
    .join(
        addr_df.filter(F.col('state').isNull() & F.col('postal_code').isNull())
               .select(
                   F.col('address_id'),
                   F.col('country').alias('a_country'),
                   F.col('city').alias('a_city'),
                   F.col('address_line').alias('a_address_line')
        ),
        F.col('country').eqNullSafe(F.col('a_country')) &
        F.col('city').eqNullSafe(F.col('a_city')) &
        F.col('address_line').eqNullSafe(F.col('a_address_line')),
        how='left'
).drop('country', 'city', 'address_line', 'a_country', 'a_city', 'a_address_line') \
    .filter(F.col('email').isNotNull()) \
    .withColumn('supplier_id', F.row_number().over(Window.orderBy('email')))

dim_supplier.write.jdbc(url=pg_url, table='dwh.dim_supplier',
                        mode='append', properties=pg_props)

cat_df = spark.read.jdbc(
    url=pg_url, table='dwh.dim_product_category', properties=pg_props)
pet_cat_df = spark.read.jdbc(
    url=pg_url, table='dwh.dim_pet_category', properties=pg_props)
brand_df = spark.read.jdbc(
    url=pg_url, table='dwh.dim_brand', properties=pg_props)

dim_product = mock_df.select(
    F.col('product_name').alias('name'),
    F.col('product_category').alias('category_name'),
    F.col('product_price').cast('decimal(10,2)').alias('price'),
    F.col('product_quantity').cast('int').alias('quantity'),
    F.col('pet_category').alias('pet_category_name'),
    F.col('product_weight').cast('decimal(10,2)').alias('weight'),
    F.col('product_color').alias('color'),
    F.col('product_size').alias('size'),
    F.col('product_brand').alias('brand_name'),
    F.col('product_material').alias('material'),
    F.col('product_description').alias('description'),
    F.col('product_rating').cast('float').alias('rating'),
    F.col('product_reviews').cast('int').alias('reviews'),
    F.to_date('product_release_date', 'M/d/yyyy').alias('release_date'),
    F.to_date('product_expiry_date', 'M/d/yyyy').alias('expiry_date'),
    F.col('sale_product_id').alias('source_product_id')
).distinct() \
    .join(cat_df.select('product_category_id', F.col('name').alias('category_name')),
          on='category_name', how='left') \
    .join(pet_cat_df.select('pet_category_id', F.col('name').alias('pet_category_name')),
          on='pet_category_name', how='left') \
    .join(brand_df.select('brand_id', F.col('name').alias('brand_name')),
          on='brand_name', how='left') \
    .drop('category_name', 'pet_category_name', 'brand_name') \
    .filter(F.col('name').isNotNull()) \
    .withColumn('product_id', F.row_number().over(Window.orderBy('name', 'price')))

dim_product.write.jdbc(url=pg_url, table='dwh.dim_product',
                       mode='append', properties=pg_props)

cust_df = spark.read.jdbc(
    url=pg_url, table='dwh.dim_customer', properties=pg_props)

dim_pet = mock_df.select(
    F.col('customer_pet_type').alias('type'),
    F.col('customer_pet_name').alias('name'),
    F.col('customer_pet_breed').alias('breed'),
    F.col('customer_email').alias('email')
).distinct() \
    .join(cust_df.select('customer_id', 'email'), on='email', how='inner') \
    .drop('email') \
    .withColumn('pet_id', F.row_number().over(Window.orderBy('type', 'name')))

dim_pet.write.jdbc(url=pg_url, table='dwh.dim_pet',
                   mode='append', properties=pg_props)

dim_cust = spark.read.jdbc(
    url=pg_url, table='dwh.dim_customer', properties=pg_props)
dim_sell = spark.read.jdbc(
    url=pg_url, table='dwh.dim_seller', properties=pg_props)
dim_prod = spark.read.jdbc(
    url=pg_url, table='dwh.dim_product', properties=pg_props)
dim_st = spark.read.jdbc(
    url=pg_url, table='dwh.dim_store', properties=pg_props)
dim_sup = spark.read.jdbc(
    url=pg_url, table='dwh.dim_supplier', properties=pg_props)

prod_key_df = dim_prod.alias('p') \
    .join(cat_df.alias('c'), F.col('p.product_category_id') == F.col('c.product_category_id')) \
    .join(pet_cat_df.alias('pc'), F.col('p.pet_category_id') == F.col('pc.pet_category_id')) \
    .join(brand_df.alias('b'), F.col('p.brand_id') == F.col('b.brand_id')) \
    .select(
        F.col('p.product_id'),
        F.col('p.source_product_id'),
        F.col('p.name'),
        F.col('c.name').alias('category_name'),
        F.col('p.price'),
        F.col('p.quantity'),
        F.col('pc.name').alias('pet_category_name'),
        F.col('p.weight'),
        F.col('p.color'),
        F.col('p.size'),
        F.col('b.name').alias('brand_name'),
        F.col('p.material'),
        F.col('p.description'),
        F.col('p.rating'),
        F.col('p.reviews'),
        F.col('p.release_date'),
        F.col('p.expiry_date')
)

fact_sale = mock_df.alias('md') \
    .join(dim_cust.alias('c'), F.col('md.customer_email') == F.col('c.email'), 'left') \
    .join(dim_sell.alias('s'), F.col('md.seller_email') == F.col('s.email'), 'left') \
    .join(
        prod_key_df.alias("p"),
        (F.col("md.sale_product_id") == F.col("p.source_product_id")) &
        (F.col("md.product_name") == F.col("p.name")) &
        (F.col("md.product_category") == F.col("p.category_name")) &
        (F.col("md.product_price").cast("decimal(10,2)") == F.col("p.price")) &
        (F.col("md.product_quantity").cast("int") == F.col("p.quantity")) &
        (F.col("md.pet_category") == F.col("p.pet_category_name")) &
        (F.col("md.product_weight").cast("decimal(10,2)") == F.col("p.weight")) &
        (F.col("md.product_color") == F.col("p.color")) &
        (F.col("md.product_size") == F.col("p.size")) &
        (F.col("md.product_brand") == F.col("p.brand_name")) &
        (F.col("md.product_material") == F.col("p.material")) &
        (F.col("md.product_description") == F.col("p.description")) &
        (F.col("md.product_rating").cast("float") == F.col("p.rating")) &
        (F.col("md.product_reviews").cast("int") == F.col("p.reviews")) &
        (F.to_date("md.product_release_date", "M/d/yyyy") == F.col("p.release_date")) &
        (F.to_date("md.product_expiry_date", "M/d/yyyy") == F.col("p.expiry_date")),
        'left'
) \
    .join(dim_st.alias("st"), F.col("md.store_email") == F.col("st.email"), 'left') \
    .join(dim_sup.alias("su"), F.col("md.supplier_email") == F.col("su.email"), "left") \
    .select(
        F.to_date("md.sale_date", "M/d/yyyy").alias("sale_date"),
        F.col("c.customer_id"),
        F.col("s.seller_id"),
        F.col("p.product_id"),
        F.col("st.store_id"),
        F.col("su.supplier_id"),
        F.col("md.sale_quantity").alias("quantity"),
        F.col("md.sale_total_price").cast("decimal(10,2)").alias("total_price")
)

fact_sale.write.jdbc(url=pg_url, table="dwh.fact_sale",
                     mode="append", properties=pg_props)

spark.stop()
