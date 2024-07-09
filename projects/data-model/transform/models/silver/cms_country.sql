SELECT
    id as country_id,
    name as country_name
FROM {{source('cms', 'country')}}