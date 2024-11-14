with 

companies as (
    select * from {{ ref('cms__companies') }}
)

, countries as (
    select * from {{ ref('cms__countries') }}
)


, all_tables_joined as (
    select 
        companies.company_id
        , companies.company_name
        , countries.country_id
        , countries.country_name
        , countries.language_id
        , countries.country_currency
    from companies
    left join countries on countries.country_id = companies.company_id
)

select * from all_tables_joined