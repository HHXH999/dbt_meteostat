-- models/staging/stg_customers_demo.sql

SELECT
    customer_id,
    name
FROM {{ source('s_zhenyang', 'customers_raw') }}
