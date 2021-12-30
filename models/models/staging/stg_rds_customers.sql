WITH source AS (
    select * from {{source('rds', 'customers')}}
), new_table AS (
    SELECT 'rds-' || customer_id as customer_id, 
    split_part(contact_name, ' ', 1) as first_name, 
    split_part(contact_name, ' ', 2) as last_name, 
    TRANSLATE(phone, '(),./ -', '') as cleaned_phone,
    CASE
        WHEN LENGTH(cleaned_phone) = 10
        THEN '(' || SUBSTRING(cleaned_phone, 1, 3) || ') ' || SUBSTRING(cleaned_phone, 4, 3) || '-' || SUBSTRING(cleaned_phone, 7,4)
        ELSE NULL
    END AS customer_phone,
    phone, 
    company_name

    FROM source
), company_info AS (
    SELECT * FROM GET99NVI3S.stg_rds_companies
), final_query AS (
    SELECT customer_id as contact_id,
    first_name,
    last_name,
    customer_phone,
    company_id
    FROM company_info JOIN new_table ON company_info.company_name = new_table.company_name
)
SELECT * FROM final_query;

