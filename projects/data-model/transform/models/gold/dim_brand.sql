{{ config(alias='dim_brand')}}

SELECT
    md5(company_id) AS pk_dim_brand,
    company_id,
    company_name,
    country_name
FROM {{ref("cms_company")}} company
LEFT JOIN {{ref("cms_country")}} country
ON company.country_id = country.country_id