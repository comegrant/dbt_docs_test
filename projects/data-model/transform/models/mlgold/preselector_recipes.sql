with

recommendations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}
    -- only include the most recent recommendation for a menu week
    where recommendation_version_desc = 1

)

, menus as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, agreements as (

    select * from {{ ref('cms__billing_agreements') }}
    -- are currently null for the active row, but will be changed to future proof date soon
    where valid_to is null or valid_to = '{{ var("future_proof_date") }}'

)

, recommendation_engine_preselected_recipes as (
    select
        recommendations.menu_week_monday_date
        , recommendations.menu_week
        , recommendations.menu_year
        , recommendations.billing_agreement_basket_id
        , recommendations.billing_agreement_id
        , recommendations.product_variation_id
        , recommendations.deviation_created_at
        , recommendations.deviation_created_by
        , menus.recipe_id
    from recommendations
    left join agreements
        on recommendations.billing_agreement_id = agreements.billing_agreement_id
    left join menus
        on
            recommendations.menu_week_monday_date = menus.menu_week_monday_date
            and recommendations.product_variation_id = menus.product_variation_id
            and agreements.company_id = menus.company_id
    left join products
        on
            recommendations.product_variation_id = products.product_variation_id
            and agreements.company_id = products.company_id
    where
        product_type_id = '{{ var("velg&vrak_product_type_id") }}'
        and recommendations.billing_agreement_basket_deviation_origin_id
        = '{{ var("preselector_origin_id") }}'
)

select * from recommendation_engine_preselected_recipes
