with 

addresses as (

    select * from {{ source('cms', 'cms__address_live') }}

)

-- filter out postal codes with a character length of 1 that are not pickup locations
, addresses_filtered as (
    select *
    from addresses
    where postal_code in (
        {% for country in var('countries').values() -%}
        {%- if not loop.first %}, {% endif %}'{{ country["pickup_location_postal_code_id"] }}'
        {%- endfor %}
    )
    or length(postal_code) <> 1

)

-- rename columns in addresses to follow naming convention
-- and clean postal code to have the right format
, addresses_renamed as (

    select
        
        {# ids #}
        id as shipping_address_id
        , cast(postal_code as int) as postal_code_id

        {# ints #}
        , agreement_id as billing_agreement_id

        {# booleans #}
        , geo_restricted as is_geo_restricted

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by
        
    from addresses_filtered

)

select * from addresses_renamed