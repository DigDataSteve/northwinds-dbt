{{ config(materialized='table') }}
WITH int_companies AS (
    SELECT * FROM {{ ref('int_companies')}}
)
SELECT company_pk, company_name, city, address, postal_code FROM   int_companies