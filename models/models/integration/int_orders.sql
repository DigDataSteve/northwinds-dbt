WITH order_info AS (
    SELECT order_id as org_order_id, order_date, customer_id, employee_id FROM {{source('rds', 'orders')}}
), order_details_info AS (
    SELECT order_id, product_id, quantity, discount, unit_price FROM {{source('rds', 'order_details')}}
), combine_order_info AS (
    SELECT 
    'rds-' || org_order_id as order_id, order_date, 
    'rds-' || customer_id as customer_id, 
    'rds-' || employee_id as employee_id, 
    'rds-' || product_id as product_id, 
    quantity, discount, unit_price 
    FROM order_info JOIN order_details_info ON order_info.org_order_id = order_details_info.order_id
), surrogate_key AS (
    SELECT {{ dbt_utils.surrogate_key(['order_date', 'product_id', 'customer_id', 'order_id']) }} as order_pk,
    order_date, customer_id, employee_id, product_id, quantity, discount, unit_price
    FROM combine_order_info
)
SELECT * FROM surrogate_key 
-- SELECT order_pk, count(*) as total FROM surrogate_key group by order_pk order by total desc