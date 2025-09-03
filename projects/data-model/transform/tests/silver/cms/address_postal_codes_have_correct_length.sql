with 

addresses as (

    select * from {{ ref('dim_addresses') }}

)

, addresses_with_postal_code_length as (

    select 
        *
        , case
            {% for country in var('countries').values() -%}
                when country_id = '{{ country["country_id"] }}'
                then {{ country["postal_code_length"] }}
            {% endfor -%}
            else null 
            end as num_postal_code_characters
    from addresses

)

select 
    *
from addresses_with_postal_code_length
where len(postal_code) != num_postal_code_characters
and postal_code not in (
    -- ignore if postal code is an artificial postal code of a pickup location
    {% for country in var('countries').values() -%}
    {%- if not loop.first %}, {% endif %}'{{ country["pickup_location_postal_code_id"] }}'
    {%- endfor %}
)
