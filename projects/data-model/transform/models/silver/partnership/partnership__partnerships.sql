with 

source as (

    select * from {{ source('partnership', 'partnership__partnership') }}

)

, renamed as (

    select

        
        {# ids #}
        id as partnership_id

        {# strings #}
        , name as partnership_name
        
        {# booleans #}
        , has_loyalty_program
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
