with

orders as (

    select * from {{ ref('cms__billing_agreement_orders') }}

)

, order_lines as (

    select * from {{ ref('cms__billing_agreement_order_lines') }}

)

, has_delivery as (

    select * from {{ ref('int_billing_agreement_order_lines_with_delivery') }}

)

, recommendations as (

    select * from {{ ref('int_basket_deviation_recommendations_most_recent') }}
)

, deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}
)

, menus as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, billing_agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, products as (

    select * from {{ ref('dim_products') }}

)

, companies as (

    select * from {{ ref('dim_companies') }}

)

, order_line_agreements_joined as (

    select
        orders.menu_year
        , orders.menu_week
        , orders.menu_week_monday_date
        , datediff(
            WEEK
            , billing_agreements.first_menu_week_monday_date
            , orders.menu_week_monday_date) as weeks_since_first_order
        , orders.source_created_at
        , order_lines.product_variation_quantity
        , order_lines.vat
        , order_lines.unit_price_ex_vat
        , order_lines.unit_price_inc_vat
        , order_lines.total_amount_ex_vat
        , order_lines.total_amount_inc_vat
        , order_lines.order_line_type_name
        , orders.has_recipe_leaflets
        , coalesce(has_delivery.has_delivery, false) as has_delivery
        , billing_agreements.company_id
        , companies.language_id
        , orders.billing_agreement_order_id
        , billing_agreements.billing_agreement_id
        , billing_agreements.valid_from as valid_from_billing_agreements
        , orders.ops_order_id
        , orders.order_status_id
        , orders.order_type_id
        , order_lines.billing_agreement_order_line_id
        , products.product_type_id
        , products.product_id
        , products.product_variation_id
        , case 
            when products.product_type_id = '2F163D69-8AC1-6E0C-8793-FF0000804EB3' -- Mealbox
            and products.product_id != 'D699150E-D2DE-4BC1-A75C-8B70C9B28AE3' -- Onesub
            then true
            else false
        end as is_chef_composed_mealbox
        , case
            when products.product_type_id in (
                    '2F163D69-8AC1-6E0C-8793-FF0000804EB3' -- Mealbox
                    , 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1' -- Velg&Vrak
                    , '288ED8CA-B437-4A6D-BE16-8F9A30185008' -- Financial
                )
            then true
            else false
        end as is_mealbox
        , case 
            when products.product_type_id = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1' -- Velg&Vrak
            then true
            else false
        end as is_dish
    from orders
    left join order_lines
        on orders.billing_agreement_order_id = order_lines.billing_agreement_order_id
    left join has_delivery
        on orders.billing_agreement_order_id = has_delivery.billing_agreement_order_id
    left join billing_agreements
        on orders.billing_agreement_id = billing_agreements.billing_agreement_id
        and orders.source_created_at >= billing_agreements.valid_from
        and orders.source_created_at < billing_agreements.valid_to
    left join products
        on order_lines.product_variation_id = products.product_variation_id
        and billing_agreements.company_id = products.company_id
    left join companies
        on billing_agreements.company_id = companies.company_id
    -- Exclude orders without any order lines
    where billing_agreement_order_line_id is not null

)

, menu_recipes as (
    select distinct
        menu_week_monday_date
        , company_id
        , product_variation_id
        , recipe_id
    from menus
    where recipe_id is not null
)

, ordered_recipes as (

    select
        order_line_agreements_joined.billing_agreement_order_id
        , order_line_agreements_joined.billing_agreement_order_line_id
        , order_line_agreements_joined.product_variation_id
        , menu_recipes.recipe_id
        , order_line_agreements_joined.is_chef_composed_mealbox
        , order_line_agreements_joined.is_mealbox
        , order_line_agreements_joined.is_dish
    from order_line_agreements_joined
    left join menu_recipes
        on order_line_agreements_joined.menu_week_monday_date = menu_recipes.menu_week_monday_date
        and order_line_agreements_joined.product_variation_id = menu_recipes.product_variation_id
        and order_line_agreements_joined.company_id = menu_recipes.company_id
    where menu_recipes.recipe_id is not null

)

, chef_preselected_recipes as (

    select
        order_line_agreements_joined.billing_agreement_order_id
        , order_line_agreements_joined.product_variation_id
        , menu_recipes.recipe_id
    from order_line_agreements_joined
    left join products
        on order_line_agreements_joined.product_variation_id = products.product_variation_id
        and order_line_agreements_joined.company_id = products.company_id
    left join menu_recipes
        on products.preselected_mealbox_product_variation_id = menu_recipes.product_variation_id
        and order_line_agreements_joined.company_id = menu_recipes.company_id
        and order_line_agreements_joined.menu_week_monday_date = menu_recipes.menu_week_monday_date
    where menu_recipes.recipe_id is not null

)

, deviations_filter_active as (
    select 
        menu_week_monday_date
        , billing_agreement_id
        , billing_agreement_basket_id
        , product_variation_id
    from deviations
    where deviations.is_active_deviation = true
)

, deviations_order_mapping as (
    select distinct 
        deviations_filter_active.billing_agreement_basket_id
        , deviations_filter_active.menu_week_monday_date
        , order_line_agreements_joined.billing_agreement_order_id
        , order_line_agreements_joined.company_id
    from deviations_filter_active
    left join order_line_agreements_joined
        on deviations_filter_active.menu_week_monday_date = order_line_agreements_joined.menu_week_monday_date
        and deviations_filter_active.billing_agreement_id = order_line_agreements_joined.billing_agreement_id
        and deviations_filter_active.product_variation_id = order_line_agreements_joined.product_variation_id
    where order_line_agreements_joined.billing_agreement_order_id is not null
)

, recommendation_engine_preselected_recipes as (
    select
      deviations_order_mapping.billing_agreement_order_id
      , recommendations.product_variation_id
      , menu_recipes.recipe_id
    from deviations_order_mapping
    left join recommendations
        on deviations_order_mapping.billing_agreement_basket_id = recommendations.billing_agreement_basket_id
        and deviations_order_mapping.menu_week_monday_date = recommendations.menu_week_monday_date
    left join menu_recipes
        on recommendations.menu_week_monday_date = menu_recipes.menu_week_monday_date
        and recommendations.product_variation_id = menu_recipes.product_variation_id
        and deviations_order_mapping.company_id = menu_recipes.company_id
    where menu_recipes.recipe_id is not null
)

, preselected_recipes as ( 
    
    select 
        *
    from recommendation_engine_preselected_recipes
    
    union all

    -- Only includ chef_preselected_recipes for orders where the recommendation engine was not run
    select
        *
    from chef_preselected_recipes 
    where chef_preselected_recipes.billing_agreement_order_id not in (
        select distinct 
            recommendation_engine_preselected_recipes.billing_agreement_order_id 
        from recommendation_engine_preselected_recipes
        )
)

, ordered_and_preselected_recipes_joined as (

    select
        coalesce(
            ordered_recipes.billing_agreement_order_id
            , preselected_recipes.billing_agreement_order_id
        ) as billing_agreement_order_id
        , ordered_recipes.billing_agreement_order_line_id
        , ordered_recipes.recipe_id
        , preselected_recipes.recipe_id as preselected_recipe_id
        , ordered_recipes.product_variation_id
        , preselected_recipes.product_variation_id as preselected_product_variation_id
        , coalesce(ordered_recipes.is_chef_composed_mealbox, false) as is_chef_composed_mealbox
        , case 
            when ordered_recipes.is_chef_composed_mealbox is true
            then 0
            when ordered_recipes.is_dish is false
            then null
            when ordered_recipes.recipe_id is not null 
            and preselected_recipes.recipe_id is null
            then 1
            else 0
            end as is_added_recipe
        , case
            when ordered_recipes.is_chef_composed_mealbox is false
            and ordered_recipes.is_dish is false
            then null
            when ordered_recipes.recipe_id is null 
            and preselected_recipes.recipe_id is not null
            then 1
            else 0
            end as is_removed_recipe
        , case 
            when ordered_recipes.recipe_id is null 
            and preselected_recipes.recipe_id is not null
            then true
            when ordered_recipes.is_chef_composed_mealbox = true
            then true
            else false
        end as is_generated_recipe_line
    from ordered_recipes
    full join preselected_recipes
        on ordered_recipes.billing_agreement_order_id = preselected_recipes.billing_agreement_order_id
        and ordered_recipes.recipe_id = preselected_recipes.recipe_id

)

, add_recipes_to_orders as (
    
    -- Recipes that have a direct relation to an order line
    select
        order_line_agreements_joined.menu_year
        , order_line_agreements_joined.menu_week
        , order_line_agreements_joined.menu_week_monday_date
        , order_line_agreements_joined.weeks_since_first_order
        , order_line_agreements_joined.source_created_at
        , order_line_agreements_joined.product_variation_quantity
        , order_line_agreements_joined.vat
        , order_line_agreements_joined.unit_price_ex_vat
        , order_line_agreements_joined.unit_price_inc_vat
        , order_line_agreements_joined.total_amount_ex_vat
        , order_line_agreements_joined.total_amount_inc_vat
        , order_line_agreements_joined.order_line_type_name
        , order_line_agreements_joined.has_recipe_leaflets
        , order_line_agreements_joined.has_delivery
        , order_line_agreements_joined.billing_agreement_order_id
        , order_line_agreements_joined.ops_order_id
        , order_line_agreements_joined.order_status_id
        , order_line_agreements_joined.order_type_id
        , order_line_agreements_joined.company_id
        , order_line_agreements_joined.language_id
        , order_line_agreements_joined.billing_agreement_id
        , order_line_agreements_joined.valid_from_billing_agreements
        , order_line_agreements_joined.billing_agreement_order_line_id
        , order_line_agreements_joined.product_variation_id
        , ordered_and_preselected_recipes_joined.recipe_id
        , ordered_and_preselected_recipes_joined.preselected_recipe_id
        , ordered_and_preselected_recipes_joined.preselected_product_variation_id
        , ordered_and_preselected_recipes_joined.is_added_recipe
        , ordered_and_preselected_recipes_joined.is_removed_recipe
        , false as is_generated_recipe_line
        , order_line_agreements_joined.is_chef_composed_mealbox
        , is_mealbox
        , is_dish
    from order_line_agreements_joined
    left join ordered_and_preselected_recipes_joined
        on order_line_agreements_joined.billing_agreement_order_line_id = ordered_and_preselected_recipes_joined.billing_agreement_order_line_id
        and ordered_and_preselected_recipes_joined.is_generated_recipe_line = false

    union all

    -- Add recipes that does not belong to an order line:
    -- Fixed mealkit dishes and swapped out dishes
    select distinct
        order_line_agreements_joined.menu_year
        , order_line_agreements_joined.menu_week
        , order_line_agreements_joined.menu_week_monday_date
        , order_line_agreements_joined.weeks_since_first_order
        , order_line_agreements_joined.source_created_at
        , 0 as product_variation_quantity
        , 0 as vat
        , 0 as unit_price_ex_vat
        , 0 as unit_price_inc_vat
        , 0 as total_amount_ex_vat
        , 0 as total_amount_inc_vat
        , "GENERATED" as order_line_type_name
        , order_line_agreements_joined.has_recipe_leaflets
        , order_line_agreements_joined.has_delivery
        , order_line_agreements_joined.billing_agreement_order_id
        , null as ops_order_id
        , order_line_agreements_joined.order_status_id
        , order_line_agreements_joined.order_type_id
        , order_line_agreements_joined.company_id
        , order_line_agreements_joined.language_id
        , order_line_agreements_joined.billing_agreement_id
        , order_line_agreements_joined.valid_from_billing_agreements
        , null as billing_agreement_order_line_id
        , null as product_variation_id
        , ordered_and_preselected_recipes_joined.recipe_id
        , ordered_and_preselected_recipes_joined.preselected_recipe_id
        , ordered_and_preselected_recipes_joined.preselected_product_variation_id
        , ordered_and_preselected_recipes_joined.is_added_recipe
        , ordered_and_preselected_recipes_joined.is_removed_recipe
        , ordered_and_preselected_recipes_joined.is_generated_recipe_line
        , order_line_agreements_joined.is_chef_composed_mealbox
        , true as is_mealbox
        , true as is_dish
    from order_line_agreements_joined
    left join ordered_and_preselected_recipes_joined
        on order_line_agreements_joined.billing_agreement_order_id = ordered_and_preselected_recipes_joined.billing_agreement_order_id
        and order_line_agreements_joined.is_chef_composed_mealbox = ordered_and_preselected_recipes_joined.is_chef_composed_mealbox
    where ordered_and_preselected_recipes_joined.is_generated_recipe_line = true
    order by billing_agreement_order_id, source_created_at

)

, add_fks as (
    select 
        md5(concat_ws('-'
            , menu_week_monday_date
            , billing_agreement_id
            , billing_agreement_order_id
            , billing_agreement_order_line_id
            , product_variation_id
            , preselected_product_variation_id
            , recipe_id
            , preselected_recipe_id
            )
        ) as pk_fact_orders
        , add_recipes_to_orders.*
        , md5(concat(
            cast(billing_agreement_id as string),
            cast(valid_from_billing_agreements as string)
            )
        ) AS fk_dim_billing_agreements
        , md5(company_id) AS fk_dim_companies
        , cast(date_format(menu_week_monday_date, 'yyyyMMdd') as int) as fk_dim_date
        , md5(order_status_id) AS fk_dim_order_statuses
        , md5(order_type_id) AS fk_dim_order_types
        , coalesce(
            md5(
                concat(
                    product_variation_id,
                    company_id
                    )
            ), '0'
            ) as fk_dim_products
        , coalesce(
            md5(
                concat(
                    preselected_product_variation_id,
                    company_id
                    )
                ), '0'
            ) as fk_dim_products_preselected
        , coalesce(
            md5(
                cast(
                    concat(
                        recipe_id, 
                        language_id
                        ) as string
                    )
                ), '0'
            ) as fk_dim_recipes
        , coalesce(
            md5(
                cast(
                    concat(
                        preselected_recipe_id, 
                        language_id) as string
                    )
                ), '0'
            ) as fk_dim_recipes_preselected
    from add_recipes_to_orders
)

select * from add_fks