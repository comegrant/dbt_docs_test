with

source as (
    select * from {{source('cms', 'cms__company')}}
),

renamed as (
    select
        
        {# ids #}
        id as company_id
        , country_id

        {# strings #}
        , company_name

    from source
)

select * from renamed