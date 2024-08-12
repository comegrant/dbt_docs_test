with 

companies as (

    select * from {{ref("sil_cms__companies")}}
  
),

countries as (

    select * from {{ref("sil_cms__countries")}}
  
),

countries_and_companies_joined (
    select
        md5(companies.company_id) AS pk_dim_companies,
        companies.company_id,
        countries.language_id,
        companies.company_name,
        countries.country_name,
        countries.country_currency
    from companies
    left join countries
    on companies.country_id = countries.country_id
)

select * from countries_and_companies_joined