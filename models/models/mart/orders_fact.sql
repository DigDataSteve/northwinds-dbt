WITH customer_pk AS (
    select * from {{source('rds_customer_pk', 'INT_CONTACTS')}}
)
SELECT * FROM customer_pk