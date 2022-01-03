WITH customer_pk AS (
    select contact_pk from {{source('rds_customer_pk', 'INT_CONTACTS')}}
), order_info AS (
    SELECT * FROM {{ ref('int_orders')}}
)
SELECT * FROM order_info