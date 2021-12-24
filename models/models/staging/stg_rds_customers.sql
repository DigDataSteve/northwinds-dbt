WITH source AS (
    select * from {{source('rds', 'customers')}}
), new_table AS (
    SELECT customer_id, country, 
    split_part(contact_name, ' ', 1) as first_name, 
    split_part(contact_name, ' ', 2) as last_name
    FROM source
)
SELECT * FROM new_table;

