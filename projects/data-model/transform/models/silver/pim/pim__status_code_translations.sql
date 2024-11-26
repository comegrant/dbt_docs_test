with source as (

    select * from {{ source('pim', 'pim__status_codes_translations') }}

),

renamed as (

    select
        {# ids #} 
        status_code_id as status_code_id
        , language_id
        
        {# strings #}
        , status_code_name
        , status_code_description

    from source

)

select * from renamed