{{ config(materialized='table', schema='marts') }}
SELECT 
    customer_name,
    COUNT(order_id) as total_orders,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_customer_orders_value,
    MIN(order_date) as first_order,
    MAX(order_date) as most_recent_order
FROM {{ ref('stg_sales') }}
GROUP BY
    customer_name
ORDER BY 
    SUM(total_amount) DESC