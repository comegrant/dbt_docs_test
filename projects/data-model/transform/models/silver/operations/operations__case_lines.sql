{{
    config(
        materialized='incremental',
        unique_key='case_line_id',
        on_schema_change='append_new_columns'
    )
}}

with 

source as (

    select * from {{ source('operations', 'operations__case_line') }}

)

, renamed as (

    select

        {# ids #}
        case_line_id 
        , case_line_case_id as case_id
        , case_line_order_line_id as billing_agreement_order_line_id
        , case_line_type_id 
        , case_line_cause as case_cause_id
        , case_line_responsible as case_responsible_id
        , case_line_category as case_category_id
        
        {# strings #}
        , case_line_comment

        {# numerics #}
        , case_line_ammount as case_line_total_amount_inc_vat
        
        {# booleans #}
        , case_line_active as is_active_case_line

        {# system #}
        , case_line_user as source_updated_by
        , case_line_date as source_updated_at

        

    from source

)

select * from renamed
