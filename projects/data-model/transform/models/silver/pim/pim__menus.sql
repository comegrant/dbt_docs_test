with 

source as (

    select * from {{ source('pim', 'pim__menus') }}

),

renamed as (

    select
        {# ids #}
        menu_id
        , weekly_menus_id as weekly_menu_id
        , menu_external_id as product_id
        , status_code_id as menu_status_code_id
    
        {# system #}
        , created_by as system_created_by
        , created_date as system_created_date
        , modified_by as system_modified_by
        , modified_date as system_modified_date

    {# not sure if this is needed
        , status_code_id
        , menu_min_time
        , menu_max_time
        , recipe_state
        , ingredients_state
        , menu_comment
        , pdf
        , menu_selected
        , recipes_locked
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