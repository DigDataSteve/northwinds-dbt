WITH company_name AS (
    SELECT business_name FROM {{source('hubspot', 'HUBSPOT_CUSTOMER_DATA')}}
), company_info AS (
    SELECT 
    'hubspot-' || LOWER(TRANSLATE(business_name, ' ,', '-')) as company_id,
    business_name
    FROM company_name
    GROUP BY business_name
)
SELECT * FROM company_info