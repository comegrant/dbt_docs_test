with 

source as (

    select * from {{ source('product_layer', 'product_layer__product_concept') }}

),

renamed as (

    select
    
        {# ids #}
        product_concept_id

        {# strings #}
        , initcap(product_concept_name) as product_concept_name
        , product_concept_description

        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed