WITH order_info AS (
    SELECT order_id as org_order_id, order_date, customer_id, employee_id FROM NORTHWINDS.NORTHWINDS2_RDS_PUBLIC.ORDERS
), order_details_info AS (
    SELECT order_id, product_id, quantity, discount, unit_price FROM NORTHWINDS.NORTHWINDS2_RDS_PUBLIC.ORDER_DETAILS
), combine_order_info AS (
    SELECT 
    'rds-' || org_order_id as order_id, order_date, 
    'rds-' || customer_id as customer_id, 
    'rds-' || employee_id as empmloyee_id, 
    'rds-' || product_id as product_id, 
    quantity, discount, unit_price 
    FROM order_info JOIN order_details_info ON order_info.org_order_id = order_details_info.order_id
)
SELECT * FROM combine_order_info


-- order_id, order_date, employee_id, customer_id, product_id, quantity, discount, and unit_price