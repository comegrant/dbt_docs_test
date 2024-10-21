with

source as (

    select * from {{ source('pim', 'pim__menus') }}

)

, renamed as (

    select
        {# ids #} -- noqa: LT02
        menu_id
        , weekly_menus_id  as weekly_menu_id
        , menu_external_id as product_id
        , status_code_id   as menu_status_code_id

        {# booleans #}
        , menu_selected as is_selected_menu
        , recipes_locked as is_locked_recipe

        {# system #}
        , created_by       as source_created_by
        , created_date     as source_created_at
        , modified_by      as source_updated_by
        , modified_date    as source_updated_at

    {# other columns that are not added yet
        , menu_min_time
        , menu_max_time
        , recipe_state
        , ingredients_state
        , menu_comment
        , pdf
        , delivery_date
        , product_type_id
        , product_type_name
        , allow_flexible_price
        , extra_recipes
        , background_image
        , allergy_restrictions
        , menu_state
        , menu2_state
        , menu_planning_order
        , allow_campaign_price
        , dont_purchase
        , limit_basket_quantity
        , current_basket_quantity
    #}

    from source

)

select * from renamed
