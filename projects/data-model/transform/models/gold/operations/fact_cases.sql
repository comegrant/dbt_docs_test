with

cases as (

    select * from {{ref('operations__cases')}}

)

, case_lines as (

    select * from {{ref('operations__case_lines')}}

)

, case_line_ingredients as (

    select * from {{ref('operations__case_line_ingredients')}}

)

, ingredients as (

    select * from {{ref('pim__ingredients')}}

)

, orders_operations as (

    select * from {{ref('operations__orders')}}

)

, companies as (

    select * from {{ref('dim_companies')}}

)

, agreements as (

    select * from {{ref('dim_billing_agreements')}}

)

-- TODO: Need to distribute amount to ingredients
, tables_joined as (

    select
        md5(
            concat_ws('-'
                , case_lines.case_line_id
                , case_line_ingredients.ingredient_internal_reference
                -- TODO: Need to investigate further if its correct to include this
                , case_line_ingredients.product_type_id
            )
        ) as pk_fact_cases
        , cases.case_id
        , cases.ops_order_id
        , orders_operations.billing_agreement_id
        , ingredients.ingredient_id
        , companies.language_id
        , case_lines.case_line_id
        , case_lines.case_line_amount
        , case_lines.case_line_comment
        , case_lines.is_active_case_line
        , case_lines.case_line_type_id
        , case_lines.case_cause_id
        , case_lines.case_responsible_id
        , case_lines.case_category_id
        , cases.case_status_id
        , cases.redelivery_id
        , cases.redelivery_comment
        , cases.redelivery_timeblock_id
        , cases.redelivery_at
        , cases.redelivery_user
        , case_line_ingredients.ingredient_internal_reference
        , orders_operations.menu_week_financial_date
        , cases.source_updated_at as case_updated_at
        , case_lines.source_updated_at as case_line_updated_at
        , cast(date_format(orders_operations.menu_week_financial_date, 'yyyyMMdd') as int) as fk_dim_dates
        , agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
        , companies.pk_dim_companies as fk_dim_companies
        , md5(
            concat_ws(
                '-'
                , case_lines.case_line_type_id
                , case_lines.case_cause_id
                , case_lines.case_responsible_id
                , case_lines.case_category_id
            )
        ) as fk_dim_case_details
        , case 
            when case_line_ingredients.ingredient_internal_reference is null or ingredients.ingredient_id is null 
            then '0' 
            else md5(
                    concat_ws(
                        '-'
                        , ingredients.ingredient_id
                        , case_line_ingredients.ingredient_internal_reference
                        , companies.language_id
                    )
            ) 
            end as fk_dim_ingredients
    from cases
    left join case_lines
        on cases.case_id = case_lines.case_id
    left join orders_operations
        on cases.ops_order_id = orders_operations.ops_order_id
    left join agreements
        on orders_operations.billing_agreement_id = agreements.billing_agreement_id
        and orders_operations.menu_week_financial_date >= agreements.valid_from
        and orders_operations.menu_week_financial_date < agreements.valid_to
    left join companies
        on orders_operations.company_id = companies.company_id
    left join case_line_ingredients
        on case_lines.case_line_id = case_line_ingredients.case_line_id
    -- TODO: Figure out how to deal with ingredient_internal_reference that does not have a matching ingredient_id
    left join ingredients
        on case_line_ingredients.ingredient_internal_reference = ingredients.ingredient_internal_reference
    -- only include cases with case lines
    where case_lines.case_id is not null
    -- TODO: Need to handle this better
    and orders_operations.billing_agreement_id != 0

)

select * from tables_joined