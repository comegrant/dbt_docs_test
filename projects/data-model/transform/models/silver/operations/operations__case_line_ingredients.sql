with 

source as (

    select * from {{ source('operations', 'operations__case_line_ingredient') }}

)

, renamed as (

    select

        {# ids #}
        case_line_id 
        , product_type_id 
        
        {# strings #}
        , initcap(ingredient_name)  as ingredient_name
        , initcap(supplier_name) as supplier_name
        -- We are keeping internal_reference for now, but will get ingredient_id in future.
        , upper(ingredient_internal_reference) as ingredient_internal_reference 

        {# numerics #}
        , price as ingredient_price 
        , quantity as ingredient_quantity 

    from source

)

select * from renamed
