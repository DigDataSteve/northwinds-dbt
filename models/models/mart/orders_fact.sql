{{ config(materialized='table') }}
WITH customer_pk AS (
    select contact_pk, rds_contact_id from {{source('rds_customer_pk', 'INT_CONTACTS')}}
), order_info AS (
    SELECT * FROM {{ ref('int_orders')}}
), rds_customer_id AS (
    SELECT * FROM {{ ref('stg_rds_customers')}}
), final_query AS (
    SELECT order_pk, contact_pk, order_date, employee_id, product_id, quantity, discount, unit_price
    FROM customer_pk 
     JOIN order_info ON customer_pk.rds_contact_id = order_info.customer_id
     JOIN rds_customer_id ON rds_customer_id.customer_id = order_info.customer_id
     ORDER BY order_date
    
)
SELECT * FROM final_query