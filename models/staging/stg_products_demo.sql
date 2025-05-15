-- models/staging/stg_products_demo.sql

SELECT
    product_id,
    category,
    price
FROM {{ source('s_zhenyang', 'products_raw') }}
