with

recipe_ratings as (

    select * from {{ ref('pim__recipe_ratings') }}

)

, recipe_comments as (

    select * from {{ ref('pim__recipe_comments') }}

)

, quick_comments_translated as (

    select * from {{ ref('int_quick_comments_translated') }}

)

, quick_comment_stars as (

    select * from {{ ref('pim__rating_quick_comment_configuration') }}

)

, recipe_rating_quick_comments as (

    select * from {{ ref('pim__recipe_rating_quick_comments') }}

)

, max_stars as (
    
    select
        quick_comment_id
        , min(number_of_stars) as min_stars
        , max(number_of_stars) as max_stars
    from quick_comment_stars
    group by all
)

, quick_comment_sorted as (

    select 
        quick_comments_translated.quick_comment_id
        , quick_comments_translated.language_id
        , quick_comments_translated.language_name
        , array(
            {#
                I want a consistent order in quick_comment_id_combinations,
                quick_comment_combination_local and quick_comment_combination_english.
                So I'm packing them together in an array and creating an index to sort the entire array.
                Sorting by the maximum star rating
                so comments show up in order from most negative to most positive.
             #}
            row_number() over(order by max_stars.max_stars, max_stars.min_stars,  quick_comments_translated.quick_comment_id)
            , quick_comments_translated.quick_comment_id
            , quick_comments_translated.quick_comment_text_local
            , quick_comments_translated.quick_comment_text_english
        ) as quick_comment_data
    from quick_comments_translated

    left join max_stars
        on quick_comments_translated.quick_comment_id = max_stars.quick_comment_id

)

, quick_comment_combinations as (

    select 
        recipe_rating_quick_comments.recipe_rating_id
        , any_value(quick_comment_sorted.language_id) as language_id
        , any_value(quick_comment_sorted.language_name) as language_name
        , sort_array(array_agg(quick_comment_sorted.quick_comment_data)) as quick_comment_array
        , count(1) as combination_quick_comments_count
    from recipe_rating_quick_comments

    left join quick_comment_sorted
        on recipe_rating_quick_comments.quick_comment_id = quick_comment_sorted.quick_comment_id

    group by all

)

, all_tables_joined as (

    select
        coalesce(recipe_ratings.recipe_id, recipe_comments.recipe_id) as recipe_id
        , coalesce(recipe_ratings.billing_agreement_id, recipe_comments.billing_agreement_id) as billing_agreement_id
        , coalesce(recipe_ratings.recipe_rating_id, recipe_comments.recipe_rating_id) as recipe_rating_id
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
        , quick_comment_combinations.language_id
        , quick_comment_combinations.language_name
        , array_join(
            transform(quick_comment_combinations.quick_comment_array, x -> x[1])
            , ', '
        ) as quick_comment_id_combination
        , array_join(
            transform(quick_comment_combinations.quick_comment_array, x -> x[2])
            , ', '
        ) as quick_comment_combination_local
        , array_join(
            transform(quick_comment_combinations.quick_comment_array, x -> x[3])
            , ', '
        ) as quick_comment_combination_english
        , quick_comment_combinations.combination_quick_comments_count
        , md5(concat(
            coalesce(recipe_comments.recipe_comment, '')
            , coalesce(recipe_ratings.recipe_rating::string, '')
            , coalesce(quick_comment_id_combination, '')
        )) as pk_dim_recipe_reviews

    from recipe_ratings
    
    full join recipe_comments
        on recipe_ratings.recipe_rating_id = recipe_comments.recipe_rating_id

    left join quick_comment_combinations
        on recipe_ratings.recipe_rating_id = quick_comment_combinations.recipe_rating_id
)

select * from all_tables_joined
