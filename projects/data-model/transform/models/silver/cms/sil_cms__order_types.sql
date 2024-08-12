with 

source as (

    select * from {{ source('cms', 'cms_order_type') }}

),

renamed as (

    select

        {# ids #}
        id as order_type_id

        {# strings #}
        , initcap(name) as order_type_name

        {# booleans #}
        , is_direct_order
        , allow_anonymous as allows_anonymous
        , is_direct_payment

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by


    from source

)

select * from renamed