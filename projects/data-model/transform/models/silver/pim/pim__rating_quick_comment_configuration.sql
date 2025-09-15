with 

source as (

    select * from {{ source('pim', 'pim__rating_quick_comment_configuration') }}

)

, renamed as (

    select

        {# ids #}
        rating_quick_comment_configuration_id
        , quick_comment_id

        {# numerics #}
        , nr_of_stars as number_of_stars

    from source

)

select * from renamed
