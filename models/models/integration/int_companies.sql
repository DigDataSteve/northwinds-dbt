WITH rds_companies AS (
    SELECT * FROM GET99NVI3S.stg_rds_companies
), hubspot_companies AS (
    SELECT * FROM {{ ref('stg_hubspot_companies')}}
), union_companies AS (
    SELECT company_id AS rds_company_id, NULL AS hubspot_company_id, 
    company_name, city, address, postal_code FROM rds_companies
    UNION ALL
    SELECT NULL AS rds_company_id, company_id AS hubspot_company_id, 
    business_name AS company_name, NULL AS city, NULL AS address, NULL AS postal_code FROM hubspot_companies
), merged_companies AS (
    SELECT COUNT(*), MAX(hubspot_company_id), MAX(rds_company_id), company_name, 
    MAX(city) AS city, MAX(address) AS address, MAX(postal_code) AS postal_code
    FROM union_companies
    GROUP BY company_name
)
SELECT * FROM merged_companies