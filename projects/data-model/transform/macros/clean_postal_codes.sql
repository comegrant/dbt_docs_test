{% macro clean_postal_code(postal_code, country_id) %}
    case
        -- Check if the postal code is a pickup location
        when {{ postal_code }} in (
            {%- for country in var('countries').values() -%}
                {%- if not loop.first %}, {% endif %}'{{ country["pickup_location_postal_code_id"] }}'
            {%- endfor %}
        )
        then {{ postal_code }}

        -- Pad postal code with leading zeros based on country
        {%- for country in var('countries').values() %}
            when {{ country_id }} = '{{ country["country_id"] }}'
            then lpad({{ postal_code }}, {{ country["postal_code_length"] }}, '0')
        {%- endfor %}

        else {{ postal_code }}
    end
{% endmacro %}
