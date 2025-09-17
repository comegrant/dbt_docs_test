with 

source as (

    select * from {{ source('pim', 'pim__recipes_comments') }}

)

, renamed as (

    select

        
        {# ids #}
        recipe_id
        , recipe_rating_id
        , agreement_id as billing_agreement_id

        {# strings #}
        , comment as recipe_comment

        {# system #}
        , created_by as source_created_by
        , created_at as source_created_at
        , modified_by as source_updated_by
        , modified_at as source_updated_at

    from source

)

select * from renamed
