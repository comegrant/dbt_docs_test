with 

source as (

    select * from {{ source('pim', 'pim__quick_comments') }}

)

, renamed as (

    select

        {# ids #}
        quick_comment_id
        , company_id

        {# booleans #}
        , frontend_enabled as is_frontend_enabled

        {# system #}
        , created_at as source_created_at
        , modified_at as source_updated_at

    from source

)

select * from renamed
