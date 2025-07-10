with 

source as (

    select * from {{ source('operations', 'operations__orders') }}

)

, history as (

    select * from {{ ref('base_operations__orders_history') }}

)

, renamed as (

    select

        
        {# ids #}
        -- place ids here
        cast(order_id as bigint) as ops_order_id
        , cast(agreement_id as int) as billing_agreement_id
        , bao_id as billing_agreement_order_id
        , company_id
        , country_id
        , cast(agreement_timeblock as int) as timeblock_id
        , null as zone_id
        , agreement_postalcode as postal_code_id

        {# numerics #}
        , year_nr as menu_year
        , week_nr as menu_week

        {# date #}
        , {{ get_iso_week_start_date('year_nr', 'week_nr') }} as menu_week_monday_date
        
        {# scd #}
        , period_nr as menu_year_week
        
        {# system #}
        , created_by as source_created_by
        , created_date as source_created_at
        , modified_by as source_updated_by
        , modified_date as source_updated_at

    from source

)

, union_with_history as (

    select
        * 
        , {{ get_financial_date_from_monday_date('menu_week_monday_date') }} as menu_week_financial_date
    from renamed

    union

    select
        ops_order_id
        , billing_agreement_id
        , billing_agreement_order_id
        , company_id
        , country_id
        , timeblock_id
        , zone_id
        , postal_code_id
        , menu_year
        , menu_week
        , menu_week_monday_date
        , null as menu_year_week
        , source_created_by
        , source_created_at
        , source_updated_by
        , source_updated_at
        , {{ get_financial_date_from_monday_date('menu_week_monday_date') }} as menu_week_financial_date

    from history
)



select * from union_with_history
