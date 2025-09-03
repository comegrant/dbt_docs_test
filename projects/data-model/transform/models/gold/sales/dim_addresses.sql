with 

addresses as (

    select * from {{ ref('cms__addresses') }}

)

, agreements as (

    select * from {{ ref('cms__billing_agreements') }}
    where valid_to = '{{ var("future_proof_date") }}'

)

, companies as (

    select * from {{ ref('cms__companies') }}

)

, addresses_distinct as (

    select distinct
        addresses.postal_code_id
        , companies.country_id
    from addresses
    -- only include billing agreements that exist in dim billing agreements
    inner join agreements
        on addresses.billing_agreement_id = agreements.billing_agreement_id
    left join companies 
        on agreements.company_id = companies.company_id

)

, addresses_format_postal_code as (

    select
        md5(concat(cast(postal_code_id as string), country_id)) as pk_dim_addresses
        , addresses_distinct.*
        , {{ clean_postal_code('postal_code_id', 'country_id') }} as postal_code
    from addresses_distinct

)

select * from addresses_format_postal_code