with 

source as (

    select * from {{ source('pim', 'pim__ingredient_price') }}

)

, renamed as (

    select

        {# ids #}
        ingredient_price_id
        , ingredient_id
        , type as ingredient_price_type_id

        {# numerics #}
        , cost_unit as ingredient_unit_cost
        , variable_cost_unit as ingredient_unit_cost_markup
        
        {# date #}
        , price_from as ingredient_price_valid_from
        , case
            when price_to is null then cast( {{ get_scd_valid_to() }} as date)
            else price_to
            end as ingredient_price_valid_to
        
        {# system #}
        , created_by as source_created_by
        , created_date as source_created_at
        , modified_by as source_updated_by
        , modified_date as source_updated_at

    from source

)

select * from renamed
