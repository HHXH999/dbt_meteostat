-- 分层架构 (Layered Architecture): dbt 项目通常采用分层的方法来组织模型，例如：
    -- Sources (源): 定义原始数据表。
    -- Staging (暂存层): 暂存模型通常是对原始数据源进行第一层基础清洗、重命名、类型转换，通常每个源表对应一个暂存模型。
        -- "staging" (暂存/准备) 层或者一个测试性的清洗模型
    -- Intermediate (中间层): 进行更复杂的转换、连接、聚合，为最终的数据集市做准备。
    -- Marts (数据集市层): 构建最终给业务用户或 BI 工具使用的、面向特定业务领域或分析需求的数据模型。
    
-- CTE 1: orders_cleaned
WITH orders_cleaned AS (
    SELECT
        order_id, order_date, customer_id, product_id, quantity
    FROM {{ source('s_zhenyang', 'orders_raw') }} -- 从名为 's_zhenyang' 的源中读取 'orders_raw' 表
),

-- CTE 2: customers_cleaned
customers_cleaned AS (
    SELECT
        customer_id, name
    FROM {{ source('s_zhenyang', 'customers_raw') }} -- 从 's_zhenyang' 源读取 'customers_raw' 表
),

-- CTE 3: products_cleaned
products_cleaned AS (
    SELECT
        product_id, category, price
    FROM {{ source('s_zhenyang', 'products_raw') }} -- 从 's_zhenyang' 源读取 'products_raw' 表
),

-- CTE 4: order_enriched (进行连接和计算)
order_enriched AS (
    SELECT
        o.order_id,
        o.order_date,
        o.customer_id,
        c.name AS customer_name,       -- 获取客户名称
        p.category AS product_category,-- 获取产品类别
        p.price AS product_price,      -- 获取产品价格
        o.quantity,
        (o.quantity * p.price) AS total_value -- 计算总价值
    FROM
        orders_cleaned AS o
    JOIN
        customers_cleaned AS c ON o.customer_id = c.customer_id -- 连接订单和客户
    JOIN
        products_cleaned AS p ON o.product_id = p.product_id   -- 连接订单和产品
)

-- 最终 SELECT 语句
SELECT *
FROM order_enriched
