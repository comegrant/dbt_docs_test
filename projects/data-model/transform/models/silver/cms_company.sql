SELECT
    id as company_id,
    company_name,
    country_id
FROM {{source('cms', 'brand')}}