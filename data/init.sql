\set ON_ERROR_STOP on
\pset pager off

BEGIN;

SET client_encoding = 'UTF8';

-- 1. базовые таблицы + таблица для резульататов

CREATE TABLE IF NOT EXISTS orders (
	order_id int4 NOT NULL,
    order_date timestamp NOT NULL,
    customer_id int4 NOT NULL,
    CONSTRAINT pk_orders PRIMARY KEY  (order_id)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id int4 NOT NULL,
    product_id int4 NOT NULL,
    quantity int4 NOT NULL,
    price float8 NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id int4 NOT NULL,
    product_name text NOT NULL,
    category text NOT NULL,
    CONSTRAINT pk_products PRIMARY KEY (product_id)
);


-- 

CREATE TABLE IF NOT EXISTS product_analytics_monthly (
    product_id int4 NOT NULL,
    total_quantity int8 NOT NULL,
    total_revenue float8 NOT NULL,
    order_count int8 NOT NULL,
    avg_rating float8,
    positive_reviews int8 NOT NULL,
    negative_reviews int8 NOT NULL,
    total_reviews int8 NOT NULL,
    processing_date date NOT NULL,
    CONSTRAINT pk_product_analytics_monthly PRIMARY KEY (processing_date, product_id)
);


-- 2. для повторных прогонов

TRUNCATE TABLE order_items;
TRUNCATE TABLE orders;
TRUNCATE TABLE products;
TRUNCATE TABLE product_analytics_monthly;



-- 3. временгные таблицы для загрузки данных:


-- orders.csv

CREATE TEMP TABLE _stg_orders (order_id int4, customer_id int4, order_date_txt text);

\echo 'orders.csv:'
\copy _stg_orders(order_id, customer_id, order_date_txt) FROM '/data/orders.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"');

INSERT INTO orders(order_id, order_date, customer_id)
SELECT order_id, NULLIF(order_date_txt, '')::timestamp AS order_date, customer_id FROM _stg_orders;




-- order_items.csv:

CREATE TEMP TABLE _stg_order_items (order_item_id int4, order_id int4, product_id int4, quantity int4, price float8);

\echo 'order_items.csv:'
\copy _stg_order_items(order_item_id, order_id, product_id, quantity, price) FROM '/data/order_items.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"');


INSERT INTO order_items(order_id, product_id, quantity, price) SELECT order_id, product_id, quantity, price FROM _stg_order_items;



-- products.csv: 
CREATE TEMP TABLE _stg_products (product_id int4, product_name text, category text, price text);

\echo 'products.csv:'
\copy _stg_products(product_id, product_name, category, price) FROM '/data/products.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"');

INSERT INTO products(product_id, product_name, category) SELECT product_id, product_name, category FROM _stg_products;

COMMIT;


-- проверяем:

\echo 'test :'

SELECT * FROM orders ORDER BY order_date DESC LIMIT 5;

\echo 'ура'