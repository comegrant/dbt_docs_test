{{
    config(
        materialized='view'
    )
}}

with

all_days as (

    select * from {{ ref('fact_billing_agreements_daily') }}

)

, mondays as (

    select 
    * except (date),
    cast(date as timestamp) as date
    from all_days
    where 
        -- The logic in the measures assumes the data to be on weekly basis and as og now we only need weekly data in this view.
        --date >= current_date() - interval '30 days' or
        is_monday = true

)

select * from mondays