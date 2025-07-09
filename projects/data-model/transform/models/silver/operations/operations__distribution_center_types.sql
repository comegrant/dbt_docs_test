with 

source as (

    select * from {{ source('operations', 'operations__distribution_center_type') }}

)

, renamed as (

    select
        
        {# ids #}
        id as distribution_center_type_id
        
        {# strings #}
        , name as distribution_center_type__name

    from source

)

select * from renamed