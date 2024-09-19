with 

source as (

    select * from {{ source('pim', 'pim__ingredient_categories') }}

),

renamed as (

    select
        {# ids #}
        ingredient_category_id
        , status_code_id as ingredient_category_status_code_id
        , parent_category_id
        , country_id

        {# system #}
        , created_by as source_created_by
        , created_date as source_created_at
        , modified_by as source_updated_by
        , modified_date as source_updated_at

    from source

)

select * from renamed