WITH customer_pk AS (
    select contact_pk from {{source('rds_customer_pk', 'INT_CONTACTS')}}
), order_info AS (
    SELECT * FROM {{ ref('int_orders')}}
), rds_customer_id AS (
    SELECT * FROM {{ ref('stg_rds_customers')}}
), final_query AS (
    SELECT order_pk, contact_pk, order_date, employee_id, product_id, quantity, discount, unit_price
    FROM customer_pk 
    LEFT JOIN order_info ON customer_pk.contact_pk = order_info.customer_id
    LEFT JOIN stg_rds_customers ON stg_rds_customers.customer_id = order_info.customer_id
)
SELECT * FROM rds_customer_id