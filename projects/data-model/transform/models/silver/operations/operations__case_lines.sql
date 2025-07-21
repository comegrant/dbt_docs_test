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

-- the purpose of the case responsible and case cause columns was changed in 2025
-- cause was changed to contain the impact of the compalints 
-- and responsible was changed to contain the cause
, add_id_colums as (

    select
        renamed.*
        -- TODO: Alternative approach - using the ids of the case causes that relates to "impact"
        -- downside is that its not very robust if this changes over time, upside is that the period inbetween 
        -- the implementation will be cleaner (i.e. there was a period where both cause and impact was used)
        , case
            -- from 28-01-2025 there exist no cases
            -- using the old definition of case cause in the data
            when source_updated_at >= '2025-01-28'
            then case_cause_id
        end as case_impact_id
        , case
            -- from 28-01-2025 there exist no cases
            -- using the old definition of case cause  in the data
            when source_updated_at < '2025-01-28'
            -- generate new ids to ensure uniqueness
            then md5(concat_ws('-', 'C', case_cause_id))
            else md5(concat_ws('-', 'R', case_responsible_id))
        end as case_cause_responsible_id
    from renamed
)

select * from add_id_colums