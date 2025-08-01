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

, case_taxonomies as (

    select * from {{ref('operations__case_taxonomies')}}

)

, ingredients as (

    select * from {{ref('pim__ingredients')}}

)

-- TODO: Might need to look more into how its best to leverage this int
, order_zones as (

    select * from {{ ref('int_orders_zones_joined')}}

)

, customer_journey_segments as (

    select * from {{ ref('int_customer_journey_segments') }}

)

, companies as (

    select * from {{ref('dim_companies')}}

)

, agreements as (

    select * from {{ref('dim_billing_agreements')}}

)

, case_tables_joined as (

    select
        cases.case_id
        , cases.ops_order_id
        , cases.case_status_id
        , cases.redelivery_status_id
        , cases.redelivery_comment
        , cases.redelivery_timeblock_id
        , cases.redelivery_at
        , cases.redelivery_user
        , cases.source_updated_at as case_updated_at

        , case_lines.case_line_id
        , case_lines.case_line_total_amount_inc_vat
        , case_lines.case_line_comment
        , case_lines.is_active_case_line
        , case_lines.case_line_type_id
        , case_lines.case_cause_id
        , case_lines.case_responsible_id
        , case_lines.case_impact_id
        , case_lines.case_cause_responsible_id
        , case_lines.case_category_id
        , case_lines.source_updated_at as case_line_updated_at
    
        , case_line_ingredients.ingredient_internal_reference
        , case_line_ingredients.ingredient_price
        , case_line_ingredients.ingredient_quantity
        , case_line_ingredients.product_type_id

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

    from cases
    left join case_lines
        on cases.case_id = case_lines.case_id
    left join case_line_ingredients
        on case_lines.case_line_id = case_line_ingredients.case_line_id

)

-- find each ingredients share of the total ingredient cost
-- to be able to distribute the case line amount to each ingredient
, case_line_ingredients_total_cost_share as (

    select
        case_line_id
        , ingredient_internal_reference
        , product_type_id
        , ingredient_price * ingredient_quantity as  case_line_ingredient_amount_inc_vat
        -- if the ingredient cost is 0, distribute the cost equally to each ingredient (should never happen after week 5 2025)
        -- else calculate the ingredient cost and find the share of the total ingredient cost for the case line 
        , case
            when sum(ingredient_price * ingredient_quantity) over (partition by case_line_id) = 0
            then 1 / count(*) over (partition by case_line_id)
            else (ingredient_price * ingredient_quantity) / sum(ingredient_price * ingredient_quantity) over (partition by case_line_id) 
        end as  case_line_ingredient_amount_share
    from case_line_ingredients

)

, cases_ingredient_costs_distributed as (

    select 
        case_tables_joined.*
        , case_line_ingredients_total_cost_share.case_line_ingredient_amount_inc_vat
        , case_line_ingredients_total_cost_share.case_line_ingredient_amount_share
        -- distribute case line cost to ingredients
        , case 
                when case_tables_joined.ingredient_internal_reference is null -- no ingredient
                or case_tables_joined.case_line_type_id = 6 -- no credit
                then case_tables_joined.case_line_total_amount_inc_vat
                else case_tables_joined.case_line_total_amount_inc_vat * case_line_ingredients_total_cost_share.case_line_ingredient_amount_share
                end as credit_amount_inc_vat
    from case_tables_joined
    left join case_line_ingredients_total_cost_share
        on case_tables_joined.case_line_id = case_line_ingredients_total_cost_share.case_line_id
        and case_tables_joined.ingredient_internal_reference = case_line_ingredients_total_cost_share.ingredient_internal_reference
        and case_tables_joined.product_type_id = case_line_ingredients_total_cost_share.product_type_id

)


, add_keys as (

    select
        md5(
            concat_ws('-'
                , cases_ingredient_costs_distributed.case_line_id
                , cases_ingredient_costs_distributed.ingredient_internal_reference
                , cases_ingredient_costs_distributed.product_type_id
            )
        ) as pk_fact_cases
        
        , cases_ingredient_costs_distributed.*
        , cases_ingredient_costs_distributed.credit_amount_inc_vat / (1 + companies.main_vat_rate) as credit_amount_ex_vat
        
        -- TODO: Temp until we figured out how to deal with taxonomies
        , max(case_taxonomies.taxonomy_id) as taxonomy_id
        
        , order_zones.menu_week_financial_date
        , order_zones.menu_week
        , order_zones.menu_year
        
        , order_zones.billing_agreement_id
        , order_zones.billing_agreement_order_id
        , ingredients.ingredient_id
        , companies.language_id

        , cast(date_format(order_zones.menu_week_financial_date, 'yyyyMMdd') as int) as fk_dim_dates
        , coalesce(agreements.pk_dim_billing_agreements, '0') as fk_dim_billing_agreements
        , companies.pk_dim_companies as fk_dim_companies
        , md5(
            concat_ws(
                '-'
                , cases_ingredient_costs_distributed.case_line_type_id
                , cases_ingredient_costs_distributed.case_category_id
                , cases_ingredient_costs_distributed.case_impact_id
                , cases_ingredient_costs_distributed.case_cause_responsible_id
                -- TODO: Temp until we figured out how to deal with taxonomies
                , max(case_taxonomies.taxonomy_id)
            )
        ) as fk_dim_case_details
        , md5(cast(customer_journey_segments.sub_segment_id as string)) as fk_dim_customer_journey_segments
        , case 
            when cases_ingredient_costs_distributed.ingredient_internal_reference is null
            then '0' 
            else md5(
                    concat_ws(
                        '-'
                        , ingredients.ingredient_id
                        , companies.language_id
                    )
            ) 
            end as fk_dim_ingredients
        , coalesce(md5(cast(order_zones.zone_id as string)), '0') as fk_dim_transportation

    from cases_ingredient_costs_distributed
    -- TODO: Temp until we figured out how to deal with taxonomies
    left join case_taxonomies
        on cases_ingredient_costs_distributed.case_id = case_taxonomies.case_id
    left join order_zones
        on cases_ingredient_costs_distributed.ops_order_id = order_zones.ops_order_id
    left join agreements
        on order_zones.billing_agreement_id = agreements.billing_agreement_id
        and order_zones.menu_week_financial_date >= agreements.valid_from
        and order_zones.menu_week_financial_date < agreements.valid_to
    left join companies
        on order_zones.company_id = companies.company_id
    left join ingredients
        on cases_ingredient_costs_distributed.ingredient_internal_reference = ingredients.ingredient_internal_reference
    left join customer_journey_segments
        on order_zones.billing_agreement_id = customer_journey_segments.billing_agreement_id
        and order_zones.menu_week_financial_date >= customer_journey_segments.menu_week_monday_date_from
        and order_zones.menu_week_financial_date < customer_journey_segments.menu_week_monday_date_to
    -- only include cases with case lines
    where cases_ingredient_costs_distributed.case_id is not null
    -- only include case lines that are not deleted in the UI of OPS systems
    and cases_ingredient_costs_distributed.is_active_case_line is true
    -- remove God Matlyst cases
    and order_zones.company_id != '1A6819EF-CFD1-43E1-BBB0-F49001AE5562'
    -- TODO: Temp until we figured out how to deal with taxonomies
    group by all

)

select * from add_keys