with 

source as (

    select * from {{ source('pim', 'pim__recipe_rating_quick_comments') }}

)

, renamed as (

    select

        {# ids #}
        recipe_rating_id as recipe_review_id
        , quick_comment_id

    from source

)

select * from renamed
