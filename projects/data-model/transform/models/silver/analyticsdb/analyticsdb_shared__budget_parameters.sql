with 

source as (

    select * from {{ source('analyticsdb', 'analyticsdb_shared__budget_parameter') }}

)

, renamed as (

    select

        
        {# ids #}
        id as budget_parameter_id

        {# strings #}
        , initcap(name) as budget_parameter_name
        , initcap(description) as budget_parameter_description

        {# source #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by
        

    from source

)

select * from renamed
