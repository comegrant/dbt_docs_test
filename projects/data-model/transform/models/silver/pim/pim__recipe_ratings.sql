with 

source as (

    select * from {{ source('pim', 'pim__recipes_rating') }}

)

, renamed as (

    select

        {# ids #}
        recipe_id
        , agreement_id as billing_agreement_id

        {# numerics #}
        , rating as recipe_rating
        , case when rating != 0 then 1.00 * (rating-1) * (1/4) * 100 else null end as recipe_rating_score
        , case when rating = 0 then true else false end as is_not_cooked_dish
        
        {# system #}    
        , created_by as source_created_by
        , created_at as source_created_at
        , modified_by as source_updated_by
        , modified_at as source_updated_at

    from source

)

select * from renamed
