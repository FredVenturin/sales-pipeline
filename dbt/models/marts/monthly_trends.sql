{{ config(materialized='table', schema='marts') }}
WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        COUNT(order_id)                 AS total_orders,
        SUM(total_amount)               AS total_revenue
    FROM {{ ref('stg_sales') }}
    GROUP BY DATE_TRUNC('month', order_date)
    ORDER BY DATE_TRUNC('month', order_date)
)

SELECT
    month,
    total_orders,
    total_revenue,
    LAG(total_revenue) OVER (ORDER BY month) AS prev_month_revenue,
    ROUND(
        (
            (total_revenue - LAG(total_revenue) OVER (ORDER BY month))
            / LAG(total_revenue) OVER (ORDER BY month) * 100
        )::numeric
    , 1) AS revenue_growth_pct
FROM monthly_revenue
ORDER BY month