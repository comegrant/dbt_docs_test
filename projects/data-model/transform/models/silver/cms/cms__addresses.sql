with 

source as (

    select * from {{ source('cms', 'cms__address_live') }}

)

, source_filtered as (
    -- filter out postal_codes with a character length of 1 and are not pickup locations
    select *
    from source
    where postal_code in (
        {% for country in var('countries').values() -%}
        {%- if not loop.first %}, {% endif %}'{{ country["pickup_location_postal_code_id"] }}'
        {%- endfor %}
    )
    or length(postal_code) <> 1

)

, agreements as (

    select * from {{ ref('cms__billing_agreements') }}
    where valid_to = '{{ var("future_proof_date") }}'

)

, companies as (

    select * from {{ ref('cms__companies') }}

)

, agreements_and_countries as (
    select 
        agreements.billing_agreement_id
        , companies.country_id
    from agreements
    left join companies on agreements.company_id = companies.company_id
)

, source_renamed as (

    select
        
        {# ids #}
        id as shipping_address_id

        {# ints #}
        , agreement_id as billing_agreement_id
        , postal_code

        {# booleans #}
        , geo_restricted as is_geo_restricted

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by
        
    from source_filtered

)

, renamed_source_with_cleaned_postal_codes (

    select 

        source_renamed.shipping_address_id
        , source_renamed.billing_agreement_id
        , case 
            when source_renamed.postal_code in (
                {% for country in var('countries').values() -%}
                {%- if not loop.first %}, {% endif %}'{{ country["pickup_location_postal_code_id"] }}'
                {%- endfor %}
            ) 
            then source_renamed.postal_code
            {% for country in var('countries').values() -%}
                when agreements_and_countries.country_id = '{{ country["country_id"] }}'
                then lpad(source_renamed.postal_code, '{{ country["postal_code_length"] }}', '0')

            {%- endfor %}
            else source_renamed.postal_code
            end as postal_code
        , source_renamed.is_geo_restricted
        , source_renamed.source_created_at
        , source_renamed.source_created_by
        , source_renamed.source_updated_at
        , source_renamed.source_updated_by

    from source_renamed
    left join agreements_and_countries 
        on source_renamed.billing_agreement_id = agreements_and_countries.billing_agreement_id
)

select * from renamed_source_with_cleaned_postal_codes