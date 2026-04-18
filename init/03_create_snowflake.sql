create table if not exists dwh.dim_address 
(
	address_id serial primary key,
	country varchar(100),
	postal_code varchar(50),
	city varchar(100),
	state varchar(100),
	address_line varchar(100)
);

create table if not exists dwh.dim_brand (
	brand_id serial primary key,
	name varchar(100)
);

create table if not exists dwh.dim_product_category
(
	product_category_id serial primary key,
	name varchar(50)
);

create table if not exists dwh.dim_pet_category
( 
	pet_category_id serial primary key,
	name varchar(50)
);

create table if not exists dwh.dim_customer
( 
	customer_id serial primary key,
	first_name varchar(100),
	last_name varchar(100),
	age int,
	email varchar(100),
	address_id int references dwh.dim_address (address_id)
);

create table if not exists dwh.dim_seller 
( 
	seller_id serial primary key,
	first_name varchar(100),
	last_name varchar(100),
	email varchar(100),
	address_id int references dwh.dim_address (address_id)
);

create table if not exists dwh.dim_store
(
	store_id serial primary key,
	name varchar(100),
	phone varchar(50),
	email varchar(100),
	address_id int references dwh.dim_address(address_id)
);

create table if not exists dwh.dim_supplier 
(
	supplier_id serial primary key,
	name varchar(100),
	contact varchar(100),
	email varchar(100),
	phone varchar(50),
	address_id int references dwh.dim_address (address_id)
);
 
create table if not exists dwh.dim_pet
(
	pet_id serial primary key,
	type varchar(50),
	name varchar(100),
	breed varchar(50),
	customer_id int references dwh.dim_customer (customer_id)
);

create table if not exists dwh.dim_product (
    product_id serial primary key,
    name varchar(100),
    product_category_id int references dwh.dim_product_category(product_category_id),
    price numeric(10,2),
    quantity int,
    pet_category_id int references dwh.dim_pet_category(pet_category_id),
    weight numeric(10,2),
    color varchar(50),
    size varchar(50),
    brand_id int references dwh.dim_brand(brand_id),
    material varchar(100),
    description text,
    rating real,
    reviews int,
    release_date date,
    expiry_date date,
    source_product_id int
);

create table if not exists dwh.fact_sale
(
	sale_id serial primary key,
	sale_date date,
	customer_id int references dwh.dim_customer(customer_id),
	seller_id int references dwh.dim_seller(seller_id),
	product_id int references dwh.dim_product(product_id),
	store_id int references dwh.dim_store(store_id),
	supplier_id int references dwh.dim_supplier(supplier_id),
	quantity int,
	total_price numeric(10, 2)
);