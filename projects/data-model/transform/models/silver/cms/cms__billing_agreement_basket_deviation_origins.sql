with 

source as (

    select * from {{ source('cms', 'cms__billing_agreement_basket_deviation_origin') }}

)

, renamed as (

    select

        {# ids #}
        id as billing_agreement_basket_deviation_origin_id

        {# strings #}
        , origin_name as basket_deviation_origin_source_name
        , case
            when id = '{{ var("mealselector_origin_id")}}' then 'Mealselector'
            when id = '{{ var("preselector_origin_id")}}' then 'Preselector'
            when id = '{{ var("normal_origin_id")}}' then 'Normal'
            else 'Unknown'
        end as basket_deviation_origin_name
        , case
            when id = '{{ var("mealselector_origin_id")}}' then 'Not Preselector'
            when id = '{{ var("preselector_origin_id")}}' then 'Preselector'
            when id = '{{ var("normal_origin_id")}}' then 'Not Preselector'
            else 'Unknown'
        end as preselector_category
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by
    
    from source

)

select * from renamed
