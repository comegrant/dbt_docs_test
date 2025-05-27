with

cases as (

    select * from {{ref('operations__cases')}}

)

, case_lines as (

    select * from {{ref('operations__case_lines')}}

)

, tables_joined as (

    select
        md5(case_lines.case_line_id) as pk_fact_cases
        , cases.case_id
        , case_lines.case_line_id
        , case_lines.case_line_amount
        , case_lines.case_line_comment
        , case_lines.is_active_case_line
        , case_lines.case_line_type_id
        , case_lines.case_cause_id
        , case_lines.case_responsible_id
        , case_lines.case_category_id
        , cases.source_updated_at as case_updated_at
        , case_lines.source_updated_at as case_line_updated_at
        , md5(
            concat_ws(
                '-'
                , case_lines.case_line_type_id
                , case_lines.case_cause_id
                , case_lines.case_responsible_id
                , case_lines.case_category_id
            )
        ) as fk_dim_case_details
    from cases
    left join case_lines
        on cases.case_id = case_lines.case_id
    -- only include cases with case lines
    where case_lines.case_id is not null

)

select * from tables_joined