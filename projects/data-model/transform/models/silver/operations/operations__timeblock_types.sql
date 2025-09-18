with 

source as (

    select * from {{ source('operations', 'operations__timeblock_type') }}

)

, renamed as (

    select

        
        {# ids #}
        timeblock_type_id

        {# strings #}
        , initcap(timeblock_type_name) as timeblock_type_name

    from source

)

select * from renamed
