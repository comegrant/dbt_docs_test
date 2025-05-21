with 

postal_codes as (

    select * from {{ ref('operations__postal_codes') }}

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
from postal_codes
left join country_postal_code_lengths
    on postal_codes.country_id = country_postal_code_lengths.country_id
where len(postal_codes.postal_code) != country_postal_code_lengths.num_postal_code_characters
and postal_codes.postal_code_id not in (
    {% for country in var('countries').values() -%}
    {%- if not loop.first %}, {% endif %}'{{ country["pickup_location_postal_code_id"] }}'
    {%- endfor %}
)

