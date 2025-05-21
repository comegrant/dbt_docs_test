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

, addresses_with_country_code as (

    select 
        addresses.*
        , companies.country_id
    from addresses
    left join agreements
        on addresses.billing_agreement_id = agreements.billing_agreement_id
    left join companies
        on agreements.company_id = companies.company_id

)

, country_postal_code_lengths as (

    select 
        *
        , case
            {% for country in var('countries').values() -%}
                when country_id = '{{ country["country_id"] }}'
                then {{ country["postal_code_length"] }}
            {% endfor -%}
            else null 
            end as num_postal_code_characters
    from {{ ref('cms__countries') }}

)

select 
    *
from addresses_with_country_code
left join country_postal_code_lengths
    on addresses_with_country_code.country_id = country_postal_code_lengths.country_id
where len(addresses_with_country_code.postal_code) != country_postal_code_lengths.num_postal_code_characters
and addresses_with_country_code.postal_code not in (
    -- ignore if postal code is an artificial postal code of a pickup location
    {% for country in var('countries').values() -%}
    {%- if not loop.first %}, {% endif %}'{{ country["pickup_location_postal_code_id"] }}'
    {%- endfor %}
)
