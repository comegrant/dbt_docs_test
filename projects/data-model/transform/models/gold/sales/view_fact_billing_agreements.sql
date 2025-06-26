{{
    config(
        materialized='view'
    )
}}

with

all_days as (

    select * from {{ ref('fact_billing_agreements_daily') }}

)

, last_30_days_or_mondays as (

    select 
    * except (date),
    date as date_
    from all_days
    where 
        date >= current_date() - interval '30 days'
        or is_monday = true

)

select * from last_30_days_or_mondays