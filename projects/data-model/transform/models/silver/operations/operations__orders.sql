with 

source as (

    select * from {{ source('operations', 'operations__orders') }}

)

, history as (

    select * from {{ ref('operations__orders_history') }}

)

, renamed as (

    select

        
        {# ids #}
        -- place ids here
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
    
        /*, agreement_postalcode
        , agreement_timeblock
        , order_status_id
        , order_id_reference
        , weekdate
        , created_by
        , created_date
        , modified_by
        , modified_date
        , order_type
        , pod_plan_company_id
        , country_id
        , external_logistics_id
        , logistic_system_id
        , has_recipe_leaflets
        , external_order_id
        , period_nr*/

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
        , menu_year
        , menu_week
        , menu_week_monday_date
        , {{ get_financial_date_from_monday_date('menu_week_monday_date') }} as menu_week_financial_date
    from history
)



select * from union_with_history
