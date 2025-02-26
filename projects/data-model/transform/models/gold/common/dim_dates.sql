{{ config(materialized='table', tags=['static']) }}

select
   cast(date_format(date, 'yyyyMMdd') as int) as pk_dim_dates
    , *
from {{ref('data_platform__dates')}}