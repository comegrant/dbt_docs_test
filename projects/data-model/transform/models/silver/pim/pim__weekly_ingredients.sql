with

source as (

    select * from {{ source('pim', 'pim__weekly_ingredients') }}

)

, renamed as (

    select

        {# ids #}
        id as weekly_ingredient_id
        , menu_variation_id
        , ingredient_id

        {# system #}
        , created_by as source_created_by
        , updated_by as source_updated_by
        , created_at as source_created_at
        , updated_at as source_updated_at

        {# numerics #}
        , year as menu_year
        , week as menu_week
        , ingredient_quantity as weekly_ingredient_quantity


        {# booleans #}
        , is_fetched_from_recipes
        , is_active as is_active_weekly_ingredient

    {# other columns that are not added yet
        , external_id
        , reference_id
        , packing_priority
        , prepackaging
        , colli_number
        , menu_leaflet_recipe_match
        , unit_price
    #}

    from source

)

select * from renamed