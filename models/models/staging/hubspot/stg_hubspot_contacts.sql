WITH source AS (
    select 
    'HUBSPOT-' || hubspot_id as contact_id,
    first_name,
    last_name,
    TRANSLATE(phone, '().,/- ', '') as cleaned_phone,
    CASE
        WHEN LENGTH(cleaned_phone) = 10 
        THEN '(' || SUBSTRING(cleaned_phone, 1, 3) || ') ' || SUBSTRING(cleaned_phone, 4, 3) || '-' || SUBSTRING(cleaned_phone, 7, 4)
        ELSE NULL
    END as customer_phone,
    phone,
    business_name
    FROM {{source('hubspot', 'HUBSPOT_CUSTOMER_DATA')}}
), companies AS (
    SELECT * FROM GET99NVI3S.stg_hubspot_companies
), final_query AS (
    SELECT 
    contact_id,
    first_name,
    last_name,
    customer_phone,
    company_id, 
    source.business_name
    FROM source
    JOIN companies ON source.business_name = companies.business_name
)
SELECT * FROM final_query;