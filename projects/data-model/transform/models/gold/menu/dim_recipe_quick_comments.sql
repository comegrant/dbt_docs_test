with 

translated_quick_comments as (

    select * from {{ ref('int_quick_comments_translated') }}

)

, quick_comment_companies as (
    
    select * from {{ ref('pim__quick_comments') }}

)

, quick_comment_star_configuration as (
    
    select * from {{ ref('pim__rating_quick_comment_configuration') }}
)

, star_range as (
    
    select 
        quick_comment_id
        , concat(
            min(number_of_stars)
            , ' - '
            , max(number_of_stars)
        ) as star_rating_range
    from quick_comment_star_configuration
    group by all

)

, final as (

    select 
        md5(quick_comment_companies.quick_comment_id::string) as pk_dim_recipe_quick_comments
        , quick_comment_companies.quick_comment_id
        , quick_comment_companies.company_id
        , star_range.star_rating_range
        , translated_quick_comments.language_id
        , translated_quick_comments.language_name
        , translated_quick_comments.quick_comment_text_local
        , translated_quick_comments.quick_comment_text_english
        , quick_comment_companies.is_frontend_enabled
    from quick_comment_companies

    left join translated_quick_comments  
        on (quick_comment_companies.quick_comment_id = translated_quick_comments.quick_comment_id)

    left join star_range
        on (quick_comment_companies.quick_comment_id = star_range.quick_comment_id)

)

select *
from final
