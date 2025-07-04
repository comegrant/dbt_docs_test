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

-- find each ingredients share of the ingredient price
-- to be able to distribute the case line amount to each ingredient
, find_ingredients_price_share as (

    select
        case_line_id
        , ingredient_internal_reference
        , product_type_id
        , (ingredient_price * ingredient_quantity) / sum(ingredient_price * ingredient_quantity) over (partition by case_line_id) as ingredient_price_share
    from case_line_ingredients

)

-- TODO: Need to distribute amount to ingredients
, tables_joined as (

    select
        md5(
            concat_ws('-'
                , case_lines.case_line_id
                , case_line_ingredients.ingredient_internal_reference
                , case_line_ingredients.product_type_id
            )
        ) as pk_fact_cases
        , cases.case_id
        , cases.ops_order_id
        , orders_operations.billing_agreement_id
        , orders_operations.billing_agreement_order_id
        , ingredients.ingredient_id
        , case_line_ingredients.product_type_id
        , companies.language_id
        , case_lines.case_line_id
        , case when find_ingredients_price_share.ingredient_price_share is null
            then case_lines.case_line_amount
            else case_lines.case_line_amount * find_ingredients_price_share.ingredient_price_share 
            end as case_line_amount_inc_vat
        , case when find_ingredients_price_share.ingredient_price_share is null
            then case_lines.case_line_amount / (1 + companies.main_vat_rate)
            else case_lines.case_line_amount * find_ingredients_price_share.ingredient_price_share / (1 + companies.main_vat_rate)
            end as case_line_amount_ex_vat
        , case_lines.case_line_amount as case_line_total_amount_inc_vat
        , case_lines.case_line_comment
        , case 
            when case_lines.case_line_type_id in ({{var("complaint_case_line_type_ids") | join(", ")}})
            then true
            else false
        end as is_complaint
        , case 
            when cases.redelivery_status_id = '{{var("accepted_redelivery_status_id")}}'
            and case_lines.case_line_type_id = '{{var("redelivery_case_line_type_id")}}'
            then true
            else false
        end as is_accepted_redelivery
        , case_lines.case_line_type_id
        , case_lines.case_cause_id
        , case_lines.case_responsible_id
        , case_lines.case_category_id
        , cases.case_status_id
        , cases.redelivery_status_id
        , cases.redelivery_comment
        , cases.redelivery_timeblock_id
        , cases.redelivery_at
        , cases.redelivery_user
        , case_line_ingredients.ingredient_internal_reference
        , orders_operations.menu_week_financial_date
        , cases.source_updated_at as case_updated_at
        , case_lines.source_updated_at as case_line_updated_at
        , cast(date_format(orders_operations.menu_week_financial_date, 'yyyyMMdd') as int) as fk_dim_dates
        , coalesce(agreements.pk_dim_billing_agreements, '0') as fk_dim_billing_agreements
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
            when case_line_ingredients.ingredient_internal_reference is null
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
    left join ingredients
        on case_line_ingredients.ingredient_internal_reference = ingredients.ingredient_internal_reference
    left join find_ingredients_price_share
        on case_lines.case_line_id = find_ingredients_price_share.case_line_id
        and case_line_ingredients.ingredient_internal_reference = find_ingredients_price_share.ingredient_internal_reference
        and case_line_ingredients.product_type_id = find_ingredients_price_share.product_type_id
    -- only include cases with case lines
    where case_lines.case_id is not null
    -- only include case lines that are not deleted in the UI of OPS systems
    and case_lines.is_active_case_line is true
    -- remove God Matlyst cases
    and orders_operations.company_id != '1A6819EF-CFD1-43E1-BBB0-F49001AE5562'

)

select * from tables_joined