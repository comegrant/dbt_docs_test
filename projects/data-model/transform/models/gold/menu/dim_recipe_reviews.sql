with 

recipe_ratings_joined as (

    select * from {{ ref('int_recipe_ratings_comments_joined') }}

)

, add_keys_and_dedupe as (
    select 
        pk_dim_recipe_reviews
        , recipe_rating
        , recipe_rating_score
        , is_not_cooked_dish
        , recipe_comment
        , language_id
        , language_name
        , quick_comment_id_combination
        , quick_comment_combination_local
        , quick_comment_combination_english
        , combination_quick_comments_count
    from recipe_ratings_joined
    qualify row_number() over( partition by pk_dim_recipe_reviews order by pk_dim_recipe_reviews ) = 1

)

select *
from add_keys_and_dedupe