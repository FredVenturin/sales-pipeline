{{ config(materialized='table', schema='marts') }}
SELECT 
    region, 
    DATE_TRUNC('month', order_date) AS date,
    COUNT(order_id) AS total_orders,
    SUM(total_amount) AS total_revenue
FROM {{ ref('stg_sales') }}
GROUP BY 
    region, DATE_TRUNC('month', order_date)
ORDER BY 
    region, date

