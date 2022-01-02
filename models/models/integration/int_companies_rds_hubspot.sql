with merged_companies as (
    select business_name as company_name from {{ ref('stg_hubspot_companies') }} union all
    select company_name from {{ ref('stg_rds_companies') }}
)
select company_name from merged_companies group by company_name
