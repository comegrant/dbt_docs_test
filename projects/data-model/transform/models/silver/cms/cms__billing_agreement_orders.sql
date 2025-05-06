{{
    config(
        materialized='incremental',
        unique_key='billing_agreement_order_id',
        on_schema_change='append_new_columns'
    )
}}

with 

source as (

    select * from {{ source('cms', 'cms__billing_agreement_order') }}

)

, history as (

    select * from {{ ref( 'analyticsdb_orders__historical_orders_combined') }}

)

, renamed as (

    select
        {# ids #}
        id as billing_agreement_order_id
        , order_id as ops_order_id
        , order_type as order_type_id
        , order_status_id
        , agreement_id as billing_agreement_id

        {# numerics #}
        , year as menu_year
        , week as menu_week

        {# booleans #}
        , has_recipe_leaflets

        {# dates #}
        , {{ get_iso_week_start_date('year', 'week') }} as menu_week_monday_date

        {# timestamps #}
        , created_date as source_created_at

    from source

)

-- add order history to be able to find the first order of customers
-- have not added history of order lines, as this is more complicated
-- and not that pressing. 
-- I.e., the historical orders will not be included in Fact Orders 
-- as we only include orders with order lines.
, union_with_history as (

    select

        {# ids #}
        -- generate a billing agreement order id as it does not exist in the source table
        md5(ops_order_id) as billing_agreement_order_id
        , cast(ops_order_id as bigint) as ops_order_id
        , order_type_id
        , order_status_id
        , billing_agreement_id

        {# numerics #}
        -- Assumption: delivery day will never be more than 2 days before the delivery week (i.e., earliest is Saturday)
        -- is generated from delivery_date as the delivary_week and delivery_year in the source table is adapted to
        -- the financial calendar and hence does not fit with the code base we have
        , extract('yearofweek', dateadd(day, 2, delivery_date)) as menu_year
        , extract('week', dateadd(day, 2, delivery_date)) as menu_week

        {# booleans #}
        , false as has_recipe_leaflets

        {# dates #}
        , {{ get_iso_week_start_date('menu_year', 'menu_week') }} as menu_week_monday_date

        {# timestamps #}
        -- source_created_at from hisotry does not represent when the order was created, hence use null instead
        , null as source_created_at
    
    from history

    union

    select * from renamed

)

select * from union_with_history