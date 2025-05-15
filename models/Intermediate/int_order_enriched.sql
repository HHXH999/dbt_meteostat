SELECT
    o.order_id,
    o.order_date,
    o.customer_id,
    c.name AS customer_name,
    p.category AS product_category,
    p.price AS product_price,
    o.quantity,
    o.quantity * p.price AS total_value
FROM {{ ref("stg_orders_demo") }} AS o -- ref()	dbt 内部引用模型，自动处理依赖关系和 schema
JOIN {{ ref("stg_customers_demo") }} AS c ON o.customer_id = c.customer_id
JOIN {{ ref("stg_products_demo") }} AS p ON o.product_id = p.product_id
