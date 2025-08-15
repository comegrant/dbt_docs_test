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

, add_brand_name as (

    select
        *
        , case
            when company_name = 'Linas Matkasse' then 'Linas'
            when company_name = 'Adams Matkasse' then 'Adams'
            else company_name
        end as brand_name
    from renamed
)

select * from add_brand_name
