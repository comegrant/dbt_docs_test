{{
    config(
        materialized='incremental',
        unique_key='ops_order_id',
        on_schema_change='append_new_columns'
    )
}}

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
        , country_id
        , cast(agreement_timeblock as int) as timeblock_id
        , zone_id
        , agreement_postalcode as postal_code_id


        {# numerics #}
        , year_nr as menu_year
        , week_nr as menu_week
        
        {# date #}
        , {{ get_iso_week_start_date('year_nr', 'week_nr') }} as menu_week_monday_date

        {# system #}
        , created_by as source_created_by
        , created_date as source_created_at
        , modified_by as source_updated_by
        , modified_date as source_updated_at

    from source

)

select * from renamed
