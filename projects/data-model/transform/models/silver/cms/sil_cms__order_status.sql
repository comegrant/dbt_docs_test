with 

source as (

    select * from {{ source('cms', 'cms_billing_agreement_order_status') }}

),

renamed as (

    select
        {# ids #}
        id as order_status_id

        {# strings #}
        , initcap(name) as order_status_name
        
        {# booleans #}
        , can_be_cancelled

    from source

)

select * from renamed