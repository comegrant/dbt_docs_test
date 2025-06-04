with 

source as (

    select * from {{ source('pim', 'pim__unit_labels') }}

),

renamed as (

    select
        
        {# ids #}
        unit_label_id
        , status_code_id as unit_label_status_code_id

        {# system #}
        , created_by as source_created_by
        , created_date as source_created_at
        , modified_by as source_updated_by
        , modified_date as source_updated_at

        {# not sure if these are needed
        , alternative_quantity_measure
        , unit_label_multiplier
        , unit_label_conversion_type_id
        #}

    from source

)

select * from renamed