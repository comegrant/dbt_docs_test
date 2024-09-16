with 

source as (

    select * from {{ source('cms', 'cms__billing_agreement_basket_deviation_origin') }}

)

, renamed as (

    select

        {# ids #}
        id as billing_agreement_basket_deviation_origin_id

        {# strings #}
        , origin_name as basket_deviation_origin_name
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by
    
    from source

)

select * from renamed
