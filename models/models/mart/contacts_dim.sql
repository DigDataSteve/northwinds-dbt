{{ config(materialized='table') }}
WITH int_contacts AS (
    SELECT * FROM {{ ref('int_contacts')}}
)
SELECT * from int_contacts