with 

source as (

    select * from {{ ref('scd_cms__loyalty_level_companies') }}

)

, renamed as (

    select 

        {# ids #}
        id as loyalty_level_company_id
        , company_id
        , level_id as loyalty_level_id

        {# strings #}
        , name as loyalty_level_name

        {# numerics #}
        , multiplier as order_value_multiplier
        , requirement as point_requirement

        {# scd #}
        , dbt_valid_from as valid_from
        , {{ get_scd_valid_to('dbt_valid_to') }} as valid_to
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed