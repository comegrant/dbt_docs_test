with

case_lines as (

    select * from {{ref('operations__case_lines')}}

)

, case_categories as (

    select * from {{ref('operations__case_categories')}}

)

, case_causes as (

    select * from {{ref('operations__case_causes')}}

)

, case_responsible as (

    select * from {{ref('operations__case_responsible')}}

)

, case_line_types as (

    select * from {{ref('operations__case_line_types')}}

)

, case_taxonomies as (

    select * from {{ref('operations__case_taxonomies')}}

)

, taxonomies as (

    select * from {{ref('operations__taxonomies')}}

)

, find_distinct_combination as (

    select distinct
        case_lines.case_line_type_id
        , case_lines.case_cause_id
        , case_lines.case_responsible_id
        , case_lines.case_category_id
        , case_taxonomies.taxonomy_id
    from case_lines
    left join case_taxonomies
        on case_lines.case_id = case_taxonomies.case_id
    group by all
)

, tables_joined as (

    select
        md5(
            concat_ws(
                '-'
                , find_distinct_combination.case_line_type_id
                , find_distinct_combination.case_cause_id
                , find_distinct_combination.case_responsible_id
                , find_distinct_combination.case_category_id
                , find_distinct_combination.taxonomy_id
            )
        ) as pk_dim_case_details
        , find_distinct_combination.case_category_id
        , case_categories.case_category_name
        , find_distinct_combination.case_cause_id
        , case_causes.case_cause_name
        , find_distinct_combination.case_responsible_id
        , case_responsible.case_responsible_description
        , case_responsible.case_responsibility_type
        , find_distinct_combination.case_line_type_id
        , case_line_types.case_line_type_name
        , find_distinct_combination.taxonomy_id
        , taxonomies.taxonomy_name
    from find_distinct_combination
    left join case_categories
        on find_distinct_combination.case_category_id = case_categories.case_category_id
    left join case_causes
        on find_distinct_combination.case_cause_id = case_causes.case_cause_id
    left join case_responsible
        on find_distinct_combination.case_responsible_id = case_responsible.case_responsible_id
    left join case_line_types
        on find_distinct_combination.case_line_type_id = case_line_types.case_line_type_id
    left join taxonomies
        on find_distinct_combination.taxonomy_id = taxonomies.taxonomy_id
)

select * from tables_joined