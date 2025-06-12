with 

source as (

    select * from {{ source('cms', 'cms__discount_chain') }}

)

, renamed as (

    select
        
        {# ids #}
        parent_discount_id  as discount_parent_id
        , child_discount_id as discount_id

        {# numerics #}
        , chain_order       as discount_chain_order
        
        {# booleans #}
        , is_active         as is_active_discount_chain
        
        {# system #}
        , created_at        as source_created_at

    from source

)

select * from renamed
