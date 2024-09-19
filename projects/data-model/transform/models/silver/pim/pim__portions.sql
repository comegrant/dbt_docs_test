with 

source as (

    select * from {{ source('pim', 'pim__portions') }}

),

renamed as (

    select
       {# ids #}
        portion_id
        , main_portion_id
        , status_code_id as portion_status_code_id
        
        {# ints #}
        , portion_size

        {# system #}
        , created_by as source_created_by
        , created_date as source_created_at
        , modified_by as source_updated_by
        , modified_date as source_updated_at

        {# not sure if this is needed
        , is_default
        , portion_size
        #}

    from source

)

select * from renamed