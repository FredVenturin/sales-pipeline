{{ config(materialized='table', schema='marts') }}
SELECT 
    product,
    category,
    COUNT(order_id) as total_orders_by_product,
    SUM(quantity) as total_quantity,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value
FROM {{ ref('stg_sales') }}
GROUP BY 
    product, category
ORDER BY 
    SUM(total_amount) DESC

