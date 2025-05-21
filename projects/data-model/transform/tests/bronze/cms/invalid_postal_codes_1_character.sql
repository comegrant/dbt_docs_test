select
    count(*) as invalid_postal_code_count
from {{ source('cms', 'cms__address_live') }}
where length(postal_code) = 1
and postal_code not in (
    {% for country in var('countries').values() -%}
    {%- if not loop.first %}, {% endif %}'{{ country["pickup_location_postal_code_id"] }}'
    {%- endfor %}
)
having count(*) > 12
