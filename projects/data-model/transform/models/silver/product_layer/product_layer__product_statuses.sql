with 

source as (

    select * from {{ source('product_layer', 'product_layer__product_status') }}

),

renamed as (

    select
    
        {# ids #}
        status_id as product_status_id

        {# strings #}
        , initcap(status_name) as product_status_name


    from source

)

select * from renamed