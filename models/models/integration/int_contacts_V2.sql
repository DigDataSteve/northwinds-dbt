WITH hubspot_contacts AS (
    SELECT * FROM {{ ref('stg_hubspot_contacts') }}
), rds_customers AS (
    SELECT * FROM {{ ref('stg_rds_customers')}}
), merged_company_info AS (
    SELECT * FROM {{ ref('int_companies')}}
), union_contacts AS (
    SELECT contact_id AS hubspot_contact_id, NULL AS rds_contact_id, first_name, last_name, customer_phone, 
    company_pk AS company_id
    FROM hubspot_contacts JOIN merged_company_info ON hubspot_contacts.company_id = merged_company_info.hubspot_company_id
    UNION ALL
    SELECT NULL AS hubspot_contact_id, customer_id as rds_contact_id, first_name, last_name, phone as customer_phone, 
    company_pk AS company_id 
    FROM rds_customers JOIN merged_company_info ON rds_customers.company_id = merged_company_info.rds_company_id
    ORDER BY last_name, first_name
), deduped_contacts AS (
    SELECT 
    {{ dbt_utils.surrogate_key(['first_name', 'last_name', 'customer_phone']) }} as contact_pk,
    MAX(hubspot_contact_id) AS hubspot_contact_id, 
    MAX(rds_contact_id) AS rds_contact_id, first_name, last_name, 
    MAX(customer_phone) AS customer_phone, 
    MAX(company_id) AS company_id
    -- MAX(hubspot_company_id) AS hubspot_company_id, 
    -- MAX(rds_company_id) AS rds_company_id
    FROM union_contacts
    GROUP BY last_name, first_name, customer_phone
    ORDER BY last_name 
)
SELECT * FROM deduped_contacts