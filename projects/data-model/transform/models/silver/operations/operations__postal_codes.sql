with 

source as (

    select * from {{ source('operations', 'operations__postal_codes') }}

)

, get_postal_code_string_with_correct_number_characters as (

    select 
        *
        , case
            -- loop through the countries variables to check if the postal code is a pickup location. If it is, do not transform it 
            when source.postalcode_id in (
                {% for country in var('countries').values() -%}
                {%- if not loop.first %}, {% endif %}'{{ country["pickup_location_postal_code_id"] }}'
                {%- endfor %}
            ) 
            then source.postalcode_id -- pickup location postal code ids are artificial

            -- match the country_id of the postal code. 
            -- fetch the number of characters in postal codes for that country from it's postal_code_length variable
            -- Pad leading zeros to the postal code if necessary to ensure the postal code has the correct number of characters based on the country it belongs to.
            {% for country in var('countries').values() -%}
                when source.country_id = '{{ country["country_id"] }}'
                then lpad(source.postalcode_id, '{{ country["postal_code_length"] }}', '0')

            {%- endfor %}
            else source.postalcode_id
            end as postal_code
    from source


)

, renamed as (

    select 
        {# ints #}
        postalcode_id as postal_code_id

        {# ids #}
        , country_id

        {# strings #}
        , postal_code
        , city as city_name
        , county as county_name
        , municipality as municipality_name

        {# booleans #}
        , is_active
        , has_geofence

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from get_postal_code_string_with_correct_number_characters

)

select * from renamed
