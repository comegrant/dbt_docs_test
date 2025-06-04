with 

source as (

    select * from {{ source('pim', 'pim__unit_labels_translations') }}

),

renamed as (

    select
        
        {# ids #}
        unit_label_id
        , language_id

        {# strings #}
        , unit_label_name as unit_label_short_name
        , plural_unit_label_name as unit_label_short_name_plural
        , unit_label_description as unit_label_full_name

        {# not sure if these are needed
        , use_in_pdf_generation
        #}

    from source

)

select * from renamed