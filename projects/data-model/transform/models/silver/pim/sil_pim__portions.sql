with 

source as (

    select * from {{ source('pim', 'pim_portions') }}

),

renamed as (

    select
       {# ids #}
        portion_id
        , main_portion_id
        
        {# ints #}
        , portion_size

        {# system #}
        , created_by as system_created_by
        , created_date as system_created_date
        , modified_by as system_modified_by
        , modified_date as system_modified_date

        {# not sure if this is needed
        , status_code_id
        , is_default
        , portion_size
        #}

    from source

)

select * from renamed