with source as (

    select * from {{ source('pim', 'pim__taxonomies') }}

),

renamed as (

    select
        {# ids #} 
        taxonomies_id as taxonomy_id
        , status_code_id as taxonomy_status_code_id
        , country_id
        --, preference_id

        --{# ints #}
        --, taxonomy_type
        --, taxonomy_order

        {# system #}
        , created_by as source_created_by
        , created_date as source_created_at
        , modified_by as source_updated_by
        , modified_date as source_updated_at
        
        {# booleans #} 
        , is_external as is_external_taxonomy
        --, frontend_grouping

    from source

)

select * from renamed