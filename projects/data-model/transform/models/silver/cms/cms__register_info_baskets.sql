with 

source as (

    select * from {{ source('cms', 'cms__register_info_basket') }}

)

, renamed as (

    select

        
        {# ids #}
        id as register_info_basket_id
        , agreement_id as billing_agreement_id
        , delivery_week_type as delivery_week_type_id
        , status as billing_agreement_status_id

        {# dates #}
        , to_date(start_date) as start_date

    from source

)

select * from renamed
