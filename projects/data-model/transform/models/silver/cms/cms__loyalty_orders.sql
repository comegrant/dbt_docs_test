with 

source as (

    select * from {{ source('cms', 'cms__loyalty_order') }}

),

renamed as (

    select
        
        {# ids #}
        id as loyalty_order_id
        , agreement_id as billing_agreement_id
        , order_status_id as loyalty_order_status_id

        {# ints #}
        , year as order_year
        , week as order_week
        
        {# dates #}
        , {{ get_iso_week_start_date('year', 'week') }} as order_week_monday_date

        {# timestamps #}
        , created_at as source_created_at
        , updated_at as source_updated_at

        {# strings #}
        , created_by as source_created_by
        , updated_by as source_updated_by

    from source

)

select * from renamed