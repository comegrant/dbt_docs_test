with 

quick_comment_translation as (

    select * from {{ ref('pim__quick_comment_translations') }}

)

, english_translation as (

    select 
        quick_comment_id
        , quick_comment_text as quick_comment_text_english
    from quick_comment_translation
    where language_id = {{ var('english_language_id') }}
    
)

, translated_quick_comments as (

    select
        quick_comment_translation.quick_comment_id
        , quick_comment_translation.language_id
        , case 
            when quick_comment_translation.language_id = {{ var('norwegian_language_id') }} then "Norwegian"
            when quick_comment_translation.language_id = {{ var('swedish_language_id') }} then "Swedish"
            when quick_comment_translation.language_id = {{ var('danish_language_id') }} then "Danish"
        end as language_name
        , quick_comment_translation.quick_comment_text as quick_comment_text_local
        , english_translation.quick_comment_text_english
    from quick_comment_translation

    left join english_translation  
        on (quick_comment_translation.quick_comment_id = english_translation.quick_comment_id)
    
    where language_id != {{ var('english_language_id') }}

)

select *
from translated_quick_comments