create database if not exists reports;

create table reports.product_sales (
    product_id UInt32,
    product_name String,
    category_name String,
    total_revenue Decimal(12, 2),
    total_quantity UInt32,
    avg_rating Float32,
    review_count UInt32
)  ENGINE = MergeTree()
order by product_id;

create table reports.customer_sales (
    customer_id UInt32,
    first_name String,
    last_name String,
    country String,
    total_spent Decimal(12, 2),
    avg_check Decimal(12, 2)
) ENGINE = MergeTree() 
order by customer_id;

create table reports.monthly_sales (
    year_month String,
    total_revenue Decimal(12, 2),
    total_orders UInt32,
    total_quantity UInt32,
    avg_check Decimal(12, 2),
    avg_items_per_order Float32
) ENGINE = MergeTree()
order by year_month;

create table reports.store_sales (
    store_id UInt32,
    store_name String,
    country String,
    city String,
    total_revenue Decimal(12, 2),
    total_orders UInt32,
    total_quantity UInt32,
    avg_check Decimal(12, 2)
) ENGINE = MergeTree()
order by store_id;

create table reports.supplier_sales (
    supplier_id UInt32,
    supplier_name String,
    country String,
    total_revenue Decimal(12, 2),
    total_quantity UInt32,
    avg_product_price Decimal(10, 2)
) ENGINE = MergeTree()
order by supplier_id;

create table reports.product_quality(
    product_id UInt32,
    product_name String,
    category_name String,
    rating Float32,
    review_count UInt32,
    total_quantity_sold UInt32
)  ENGINE = MergeTree()
order by product_id;