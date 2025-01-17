{{ config(materialized='table', tags=['static']) }}

select * from {{ref('int_dates_with_financial_periods')}}