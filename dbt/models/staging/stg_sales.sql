{{ config(materialized='view', schema='staging') }}
SELECT DISTINCT ON (order_id)
    order_id, 
    customer_name, 
    product, 
    category, 
    quantity, 
    unit_price, 
    region, 
    status, 
    cast(order_date as date) as order_date,
    quantity*unit_price as total_amount
FROM 
    {{source('bronze', 'bronze_sales')}}
WHERE
    unit_price IS NOT NULL and quantity >0 AND status in ('completed','pending','processing' )
