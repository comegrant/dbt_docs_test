with 

source as (

    select * from {{ source('pim', 'pim__order_ingredients') }}

),

renamed as (

    select
        {# ids #}
        order_ingredient_id
        
        {# strings #}
        , upper(ingredient_internal_reference) as ingredient_internal_reference

        {# booleans #}
        , is_main_protein
        , is_main_carbohydrate
        --, ingredient_measure_flag
        --, supplier_view_flag
        --, ingredient_name_extra_info_flag
        --, reusable_flag

        {# system #}
        , created_by as source_created_by
        , created_date as source_created_at
        , modified_by as source_updated_by
        , modified_date as source_updated_at
            
        {# numerics #}
        , order_ingredient_qty as order_ingredient_quantity
        , nutrition_units
        
    from source

)

select * from renamed