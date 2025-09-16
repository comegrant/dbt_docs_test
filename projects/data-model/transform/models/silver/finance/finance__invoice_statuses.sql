with 

source as (

    select * from {{ source('finance', 'finance__invoice_desc') }}

)

, renamed as (

    select
        {# ids #}
         invoice_status as invoice_status_id

        {# strings #}
        ,descrip_en as invoice_status_name

    from source

)

select * from renamed
