with

companies as (

    select * from {{ ref("cms__companies") }}

)

, countries as (

    select * from {{ ref("cms__countries") }}

)

, countries_and_companies_joined (

    select
        md5(companies.company_id) as pk_dim_companies
        , companies.company_id
        , companies.country_id
        , countries.language_id
        , companies.company_name
        , companies.brand_name
        , countries.country_name
        , countries.country_currency
        , countries.main_vat_rate
    from companies
    left join countries
        on companies.country_id = countries.country_id

)

select * from countries_and_companies_joined
