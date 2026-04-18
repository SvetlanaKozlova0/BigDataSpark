from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_product_sales(dim_product, fact_sale, dim_category):
    product_with_category = dim_product.alias('p').join(
        dim_category.alias('c'),
        F.col('p.product_category_id') == F.col('c.product_category_id'),
        'left'
    ).select(
        F.col('p.product_id'),
        F.col('p.name').alias('product_name'),
        F.col('c.name').alias('category_name'),
        F.col('p.rating'),
        F.col('p.reviews')
    )

    agg_facts = fact_sale.groupBy('product_id').agg(
        F.sum('total_price').alias('total_revenue'),
        F.sum('quantity').alias('total_quantity')
    )
    result = agg_facts.join(product_with_category, 'product_id', 'left').select(
        'product_id',
        'product_name',
        'category_name',
        'total_revenue',
        'total_quantity',
        F.col('rating').alias('avg_rating'),
        F.col('reviews').alias('review_count')
    )
    return result


def build_customer_sales(dim_customer, fact_sale, dim_address):
    agg_facts = fact_sale.groupBy('customer_id').agg(
        F.sum('total_price').alias('total_spent'),
        F.avg('total_price').alias('avg_check')
    )

    customer_with_address = dim_customer.join(
        dim_address, 'address_id', 'left'
    ).select('customer_id', 'first_name', 'last_name', 'country')

    result = agg_facts.join(customer_with_address, 'customer_id', 'left').select(
        'customer_id',
        'first_name',
        'last_name',
        'country',
        'total_spent',
        'avg_check'
    )
    return result


def build_monthly_sales(fact_sale):
    return fact_sale.withColumn('year_month', F.date_format('sale_date', 'yyyy-MM')) \
        .groupBy('year_month') \
        .agg(
        F.sum('total_price').alias('total_revenue'),
        F.count('*').alias('total_orders'),
        F.sum('quantity').alias('total_quantity'),
        F.avg('total_price').alias('avg_check'),
        (F.sum('quantity') / F.count('*')).alias('avg_items_per_order')
    ).orderBy('year_month')


def build_store_sales(dim_store, fact_sale, dim_address):
    agg_facts = fact_sale.groupBy('store_id').agg(
        F.sum('total_price').alias('total_revenue'),
        F.count('*').alias('total_orders'),
        F.sum('quantity').alias('total_quantity')
    )
    store_with_address = dim_store.join(
        dim_address, 'address_id', 'left'
    ).select('store_id', 'name', 'country', 'city')

    result = agg_facts.join(store_with_address, 'store_id', 'left').select(
        'store_id',
        F.col('name').alias('store_name'),
        'country',
        'city',
        'total_revenue',
        'total_orders',
        'total_quantity',
        (F.col('total_revenue') / F.col('total_orders')).alias('avg_check')
    )
    return result


def build_supplier_sales(dim_supplier, dim_product, fact_sale, dim_address):

    fact_with_product = fact_sale.alias('f').join(
        dim_product.alias('p'),
        F.col('f.product_id') == F.col('p.product_id'),
        'left'
    )

    agg = fact_with_product.groupBy('f.supplier_id').agg(
        F.sum('f.total_price').alias('total_revenue'),
        F.sum('f.quantity').alias('total_quantity'),
        F.avg('p.price').alias('avg_product_price')
    )

    supplier_with_address = dim_supplier.join(
        dim_address, 'address_id', 'left'
    ).select('supplier_id', 'name', 'country')

    result = agg.join(supplier_with_address, 'supplier_id', 'left').select(
        'supplier_id',
        F.col('name').alias('supplier_name'),
        'country',
        'total_revenue',
        'total_quantity',
        'avg_product_price'
    )
    return result


def build_product_quality(dim_product, fact_sale, dim_category):
    agg_sales = fact_sale.groupBy('product_id').agg(
        F.sum('quantity').alias('total_quantity_sold')
    )
    product_with_category = dim_product.alias('p').join(
        dim_category.alias('c'),
        F.col('p.product_category_id') == F.col('c.product_category_id'),
        'left'
    ).select(
        F.col('p.product_id'),
        F.col('p.name').alias('product_name'),
        F.col('c.name').alias('category_name'),
        F.col('p.rating'),
        F.col('p.reviews')
    )
    result = product_with_category.join(agg_sales, 'product_id', 'left').select(
        'product_id',
        'product_name',
        'category_name',
        'rating',
        F.col('reviews').alias('review_count'),
        F.coalesce('total_quantity_sold', F.lit(0)
                   ).alias('total_quantity_sold')
    )
    return result
