with

recipe_ratings as (

    select * from {{ ref('pim__recipe_ratings') }}

),

recipe_comments as (

    select * from {{ ref('pim__recipe_comments') }}

),

all_tables_joined as (

    select
      coalesce(recipe_ratings.recipe_id, recipe_comments.recipe_id) as recipe_id
    , coalesce(recipe_ratings.billing_agreement_id, recipe_comments.billing_agreement_id) as billing_agreement_id
    , case when recipe_ratings.recipe_id is not null then concat(recipe_ratings.billing_agreement_id,recipe_ratings.recipe_id) else null end as recipe_rating_id
    , case when recipe_comments.recipe_id is not null then concat(recipe_comments.billing_agreement_id,recipe_comments.recipe_id) else null end as recipe_comment_id
    , recipe_ratings.recipe_rating
    , recipe_ratings.recipe_rating_score
    , recipe_ratings.is_not_cooked_dish
    , recipe_comments.recipe_comment
    , greatest(
        recipe_ratings.source_created_at
        , recipe_ratings.source_updated_at
        , recipe_comments.source_created_at
        , recipe_comments.source_updated_at
    ) as source_updated_at

    from recipe_ratings
    
    full join recipe_comments
        on recipe_ratings.recipe_id = recipe_comments.recipe_id
            and recipe_ratings.billing_agreement_id = recipe_comments.billing_agreement_id
)

select * from all_tables_joined
