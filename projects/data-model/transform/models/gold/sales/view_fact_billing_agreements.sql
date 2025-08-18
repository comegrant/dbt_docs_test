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

    * except (date)
    
    -- Convert date to timestamp in order to use the field in incremental refresh in Power BI.
    , cast(date as timestamp) as date

    from all_days
    where 
        is_monday = true

)

select * from mondays