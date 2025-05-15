SELECT
    order_id,
    order_date::date AS order_date,
    customer_id,
    product_id,
    quantity
FROM {{ source('s_zhenyang', 'orders_raw') }}
