with 

source as (

    select * from {{ source('operations', 'operations__orders_history') }}

)

, renamed as (

    select

        
        {# ids #}
        cast(order_id as bigint) as ops_order_id
        , cast(agreement_id as int) as billing_agreement_id
        , bao_id as billing_agreement_order_id
        , company_id
        , year_nr as menu_year
        , week_nr as menu_week
        , {{ get_iso_week_start_date('year_nr', 'week_nr') }} as menu_week_monday_date

        {# strings #}
        -- place strings here

        {# numerics #}
        -- place numerics here
        
        {# booleans #}
        -- place booleans here
        
        {# date #}
        -- place dates here
        
        {# timestamp #}
        -- place timestamps here
        
        {# scd #}
        -- place slowly change dimension fields here
        
        {# system #}
        -- place system columns here
        
        /*id
        , order_status_id
        , created_by
        , created_date
        , modified_by
        , modified_date
        , route_id
        , agreement_postalcode
        , weekday
        , agreement_timeblock
        , from
        , to
        , delivery_date
        , city
        , ini_cost
        , zone_id
        , transport_company_id
        , cost1drop
        , cost2drop
        , transport_name
        , order_id_reference
        , order_type
        , pod_plan_company_id
        , last_mile_hub_distribution_center_id
        , connected_hub
        , when_to_detail_plan
        , customer_fee
        , country_id
        , external_logistics_id
        , logistics_drop_price
        , logistics_other_cost
        , logistic_system_id
        , external_order_id
        */

    from source

)

select * from renamed
