with 

source as (

    select * from {{ source('partnership', 'partnership__partnership_rule') }}

)

, renamed as (

    select

        
        {# ids #}
        id as partnership_rule_id

        {# strings #}
        , name as partnership_rule_name
        , description as partnership_rule_description

        {# ints #}
        , criteria as order_criteria
        , value as partnership_points_value
        
        {# booleans #}
        , allow_multiple_uses
        , is_cumulative
        
        {# system #}        
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by


    from source

)

select * from renamed
