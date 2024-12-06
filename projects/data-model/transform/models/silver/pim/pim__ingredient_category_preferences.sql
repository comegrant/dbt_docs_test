with

source as (

    select * from {{ source('pim', 'pim__ingredient_category_preference') }}

)

, renamed as (

    select
        {# ids #}
        ingredient_category_id
        , preference_id

    from source

)

select * from renamed
