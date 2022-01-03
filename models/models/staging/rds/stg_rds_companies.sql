WITH customer_data AS (
    SELECT * FROM NORTHWINDS.NORTHWINDS2_RDS_PUBLIC.CUSTOMERS
), company_data AS (
    SELECT country, address, city, phone, company_name, postal_code
    FROM customer_data
), clean_phone_numbers AS (
    SELECT *,
    TRANSLATE(phone, '(),./ -', '') as cleaned_phone,
    CASE 
        WHEN LENGTH(cleaned_phone) = 10
        THEN '(' || SUBSTRING(cleaned_phone, 1, 3) || ') ' || SUBSTRING(cleaned_phone, 4, 3) || '-' || SUBSTRING(cleaned_phone, 7, 4)
        ELSE NULL 
        END AS company_phone
    FROM company_data
), final_cte AS (
    SELECT 
        'rds-' || LOWER(TRANSLATE(company_name, ' ', '')) as company_id,
        company_name, address, city, postal_code, country, company_phone
        FROM clean_phone_numbers
)
SELECT * FROM final_cte