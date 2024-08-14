with 

source as (

    select * from {{ source('cms', 'cms__loyalty_level') }}

),

renamed as (

    select
        
        {# ids #}
        id as loyalty_level_id

        {# ints #}
        , level as loyalty_level_number
        
    from source

)

select * from renamed