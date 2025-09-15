with 

source as (

    select * from {{ source('pim', 'pim__quick_comment_translations') }}

)

, renamed as (

    select

        {# ids #}
        quick_comment_translation_id
        , quick_comment_id
        , language_id

        {# strings #}
        , text as quick_comment_text

    from source

)

select * from renamed
