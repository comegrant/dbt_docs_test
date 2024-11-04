with

order_lines as (

    select * from {{ ref('int_billing_agreement_order_lines_joined') }}

)

, menus as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, recommendations as (

    select * from {{ ref('int_basket_deviation_recommendations_most_recent') }}
)

, deviations_order_mapping as (

    select * from {{ ref('int_basket_deviations_order_mapping') }}

)

, bridge_subscribed_products as (

    select * from {{ ref('bridge_billing_agreements_basket_products') }}

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

, order_line_dimensions_joined as (

    select
        order_lines.*
        , products.portions
        , products.meals
        , case
            when products.product_type_id in (
                    '{{ var("mealbox_product_type_id") }}'
                    , '{{ var("financial_product_type_id") }}'
                )
            then true
            else false
        end as is_mealbox
        , case
            when products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
            then true
            else false
        end as is_dish
        , case
            when products.product_type_id = '{{ var("mealbox_product_type_id") }}'
            and products.product_id != '{{ var("onesub_product_id") }}'
            then true
            else false
        end as is_chef_composed_mealbox
        , companies.company_id
        , companies.language_id
        , billing_agreements_ordergen.pk_dim_billing_agreements as fk_dim_billing_agreements_ordergen
        , coalesce(billing_agreements_deviations.pk_dim_billing_agreements, billing_agreements_ordergen.pk_dim_billing_agreements) as fk_dim_billing_agreements_deviations
        , companies.pk_dim_companies as fk_dim_companies
        , cast(date_format(order_lines.menu_week_monday_date, 'yyyyMMdd') as int) as fk_dim_date
        , md5(order_lines.order_status_id) AS fk_dim_order_statuses
        , md5(order_lines.order_type_id) AS fk_dim_order_types
        , coalesce(products.pk_dim_products, 0) AS fk_dim_products
    from order_lines
    left join deviations_order_mapping
        on order_lines.billing_agreement_order_id = deviations_order_mapping.billing_agreement_order_id
    left join billing_agreements as billing_agreements_ordergen
        on order_lines.billing_agreement_id = billing_agreements_ordergen.billing_agreement_id
        and order_lines.source_created_at >= billing_agreements_ordergen.valid_from
        and order_lines.source_created_at < billing_agreements_ordergen.valid_to
    -- TODO: Will technically be wrong for agreements which was migrated to Onesub, were pre-selector was not run
    left join billing_agreements as billing_agreements_deviations
        on order_lines.billing_agreement_id = billing_agreements_deviations.billing_agreement_id
        and deviations_order_mapping.basket_mapping_created_at >= billing_agreements_deviations.valid_from
        and deviations_order_mapping.basket_mapping_created_at  < billing_agreements_deviations.valid_to
    left join products
        on order_lines.product_variation_id = products.product_variation_id
        and billing_agreements_ordergen.company_id = products.company_id
    left join companies
        on billing_agreements_ordergen.company_id = companies.company_id

)

, menu_recipes as (

    select distinct
        menu_week_monday_date
        , product_variation_id
        , recipe_id
        , company_id
    from menus
    where recipe_id is not null
    -- only fetch recipes after Onesub 10% launch
    and menu_week_monday_date >= '{{ var("onesub_beta_launch_date") }}'

)

-- ASSUMPTION (Pre OneSub): Not possible to have a mealbox product and velg&vrak
-- Will be wrong if customer has both mealbox and velg&vrak pre Onesub
, ordered_recipes as (

    select
        order_line_dimensions_joined.billing_agreement_order_id
        , order_line_dimensions_joined.billing_agreement_order_line_id
        , order_line_dimensions_joined.product_variation_id
        , menu_recipes.recipe_id
        , order_line_dimensions_joined.is_chef_composed_mealbox
        , order_line_dimensions_joined.is_dish
    from order_line_dimensions_joined
    left join menu_recipes
        on order_line_dimensions_joined.menu_week_monday_date = menu_recipes.menu_week_monday_date
        and order_line_dimensions_joined.product_variation_id = menu_recipes.product_variation_id
        and order_line_dimensions_joined.company_id = menu_recipes.company_id
    where menu_recipes.recipe_id is not null

)

-- Gets all the product variations that customer subscribed to when placing an order
-- ASSUMPTION: Dishes will never exist as basket products
, subscribed_product_variations_mealbox as (

    select distinct
        order_line_dimensions_joined.menu_week_monday_date
        , order_line_dimensions_joined.billing_agreement_order_id
        , order_line_dimensions_joined.company_id
        , products.product_variation_id
        , products.meals
        , products.portions
    from order_line_dimensions_joined
    left join bridge_subscribed_products
        on order_line_dimensions_joined.fk_dim_billing_agreements_deviations = bridge_subscribed_products.fk_dim_billing_agreements
    left join products
        on bridge_subscribed_products.fk_dim_products = products.pk_dim_products
    -- only fetch recipes after Onesub 10% launch
    where order_line_dimensions_joined.menu_week_monday_date >= '{{ var("onesub_beta_launch_date") }}'
    -- TODO: I am sure this can be done in a more smooth way
    and products.product_type_id = '{{ var("mealbox_product_type_id") }}'
    and order_line_dimensions_joined.is_mealbox = true

)

-- TODO: How will this affect grocery subscriptions?
-- The preselected recipes for each subscribed product variation set by the chefs
, chef_preselected_recipes as (

    select
        subscribed_product_variations_mealbox.billing_agreement_order_id
        , menu_recipes.product_variation_id
        , menu_recipes.recipe_id
    from subscribed_product_variations_mealbox
    left join menu_recipes
        on subscribed_product_variations_mealbox.menu_week_monday_date = menu_recipes.menu_week_monday_date
        and subscribed_product_variations_mealbox.product_variation_id = menu_recipes.product_variation_id
        and subscribed_product_variations_mealbox.company_id = menu_recipes.company_id
    where menu_recipes.recipe_id is not null

)

-- ASSUMPTION: There can not be a normal deviation inbetween Rec Engine runs
-- Find the preselected recipes by the recommendation engine
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

, preselected_recipes (

    select * from recommendation_engine_preselected_recipes

    union all
    
    select * from chef_preselected_recipes 
    -- Only include chef_preselected_recipes for orders where the recommendation engine was not run
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
        , coalesce(ordered_recipes.is_dish, true) as is_dish
        , case
            when ordered_recipes.is_dish = false and ordered_recipes.is_chef_composed_mealbox != true -- exclude product variations that are not dishes
            then null
            when ordered_recipes.recipe_id = preselected_recipes.recipe_id
            then 0
            when ordered_recipes.recipe_id is null and preselected_recipes.recipe_id is not null
            then 0
            when ordered_recipes.recipe_id is not null and preselected_recipes.recipe_id is null
            then 1
            else null
            end as is_added_dish
        , case
            when ordered_recipes.is_dish = false and ordered_recipes.is_chef_composed_mealbox != true -- exclude product variations that are not dishes
            then null
            when ordered_recipes.recipe_id = preselected_recipes.recipe_id
            then 0
            when ordered_recipes.recipe_id is null and preselected_recipes.recipe_id is not null
            then 1
            when ordered_recipes.recipe_id is not null and preselected_recipes.recipe_id is null
            then 0
            else null
            end as is_removed_dish
        -- Identifier for lines that will be appended to the order lines
        , case
            -- All recipes belonging to a chef composed mealbox product
            when ordered_recipes.is_chef_composed_mealbox = true
            then true
            -- All preselected recipes that does not have a matching ordered recipe
            when ordered_recipes.billing_agreement_order_line_id is null
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
        order_line_dimensions_joined.menu_year
        , order_line_dimensions_joined.menu_week
        , order_line_dimensions_joined.menu_week_monday_date
        , order_line_dimensions_joined.source_created_at
        , order_line_dimensions_joined.billing_agreement_order_id
        , order_line_dimensions_joined.ops_order_id
        , order_line_dimensions_joined.billing_agreement_order_line_id
        , order_line_dimensions_joined.product_variation_quantity
        , order_line_dimensions_joined.vat
        , order_line_dimensions_joined.unit_price_ex_vat
        , order_line_dimensions_joined.unit_price_inc_vat
        , order_line_dimensions_joined.total_amount_ex_vat
        , order_line_dimensions_joined.total_amount_inc_vat
        , order_line_dimensions_joined.order_line_type_name
        , ordered_and_preselected_recipes_joined.recipe_id
        , ordered_and_preselected_recipes_joined.preselected_recipe_id
        , case
            when order_line_dimensions_joined.is_mealbox = true
            then order_line_dimensions_joined.meals - subscribed_product_variations_mealbox.meals
            else null
        end as meal_adjustment
        , case 
            when order_line_dimensions_joined.is_dish = true or order_line_dimensions_joined.is_chef_composed_mealbox = true
            then order_line_dimensions_joined.portions - subscribed_product_variations_mealbox.portions
            else null
        end as portion_adjustment
        , ordered_and_preselected_recipes_joined.is_added_dish
        , ordered_and_preselected_recipes_joined.is_removed_dish
        , order_line_dimensions_joined.is_dish
        , order_line_dimensions_joined.is_chef_composed_mealbox
        , order_line_dimensions_joined.is_mealbox
        , order_line_dimensions_joined.has_delivery
        , order_line_dimensions_joined.has_recipe_leaflets
        , order_line_dimensions_joined.billing_agreement_id
        , order_line_dimensions_joined.company_id
        , order_line_dimensions_joined.language_id
        , order_line_dimensions_joined.order_status_id
        , order_line_dimensions_joined.order_type_id
        , order_line_dimensions_joined.product_variation_id
        , ordered_and_preselected_recipes_joined.preselected_product_variation_id
        , order_line_dimensions_joined.fk_dim_billing_agreements_ordergen
        , order_line_dimensions_joined.fk_dim_billing_agreements_deviations
        , order_line_dimensions_joined.fk_dim_companies
        , order_line_dimensions_joined.fk_dim_date
        , order_line_dimensions_joined.fk_dim_order_statuses
        , order_line_dimensions_joined.fk_dim_order_types
        , order_line_dimensions_joined.fk_dim_products
        , coalesce(
            md5(
                concat(
                    ordered_and_preselected_recipes_joined.preselected_product_variation_id,
                    order_line_dimensions_joined.company_id
                    )
                ), '0'
            ) as fk_dim_products_preselected
        , coalesce(
            md5(
                cast(
                    concat(
                        ordered_and_preselected_recipes_joined.recipe_id, 
                        order_line_dimensions_joined.language_id
                        ) as string
                    )
                ), '0'
            ) as fk_dim_recipes
        , coalesce(
            md5(
                cast(
                    concat(
                        ordered_and_preselected_recipes_joined.preselected_recipe_id, 
                        order_line_dimensions_joined.language_id
                        ) as string
                    )
                ), '0'
            ) as fk_dim_recipes_preselected
    from order_line_dimensions_joined
    left join ordered_and_preselected_recipes_joined
        on order_line_dimensions_joined.billing_agreement_order_line_id = ordered_and_preselected_recipes_joined.billing_agreement_order_line_id
        and ordered_and_preselected_recipes_joined.is_generated_recipe_line = false
    left join subscribed_product_variations_mealbox
        on order_line_dimensions_joined.billing_agreement_order_id = subscribed_product_variations_mealbox.billing_agreement_order_id
    
    union all

    -- Add recipes that does not belong to an order line:
    -- Chef composed mealbox dishes and swapped out dishes
    select distinct
        order_line_dimensions_joined.menu_year
        , order_line_dimensions_joined.menu_week
        , order_line_dimensions_joined.menu_week_monday_date
        , order_line_dimensions_joined.source_created_at
        , order_line_dimensions_joined.billing_agreement_order_id
        , order_line_dimensions_joined.ops_order_id
        , null as billing_agreement_order_line_id
        , 0 as product_variation_quantity
        , 0 as vat
        , 0 as unit_price_ex_vat
        , 0 as unit_price_inc_vat
        , 0 as total_amount_ex_vat
        , 0 as total_amount_inc_vat
        , 'GENERATED' as order_line_type_name
        , ordered_and_preselected_recipes_joined.recipe_id
        , ordered_and_preselected_recipes_joined.preselected_recipe_id
        , null as meal_adjustment
        , null as portion_adjustment
        , ordered_and_preselected_recipes_joined.is_added_dish
        , ordered_and_preselected_recipes_joined.is_removed_dish
        , true as is_dish
        , false as is_chef_composed_mealbox
        , false as is_mealbox
        , order_line_dimensions_joined.has_delivery
        , order_line_dimensions_joined.has_recipe_leaflets
        , order_line_dimensions_joined.billing_agreement_id
        , order_line_dimensions_joined.company_id
        , order_line_dimensions_joined.language_id
        , order_line_dimensions_joined.order_status_id
        , order_line_dimensions_joined.order_type_id
        -- TODO: Not sure if this should be null
        , ordered_and_preselected_recipes_joined.product_variation_id
        -- TODO: Not sure if this should be null for chef composed mealbox and added to mealbox line instead
        , ordered_and_preselected_recipes_joined.preselected_product_variation_id
        , order_line_dimensions_joined.fk_dim_billing_agreements_ordergen
        , order_line_dimensions_joined.fk_dim_billing_agreements_deviations
        , order_line_dimensions_joined.fk_dim_companies
        , order_line_dimensions_joined.fk_dim_date
        , order_line_dimensions_joined.fk_dim_order_statuses
        , order_line_dimensions_joined.fk_dim_order_types
        -- TODO: Not sure if this should be null
        , coalesce(
            md5(
                concat(
                    ordered_and_preselected_recipes_joined.product_variation_id,
                    order_line_dimensions_joined.company_id
                    )
            ), '0'
            ) as fk_dim_products
        -- TODO: Not sure if this should be null for chef composed mealbox and added to mealbox line instead
        , coalesce(
            md5(
                concat(
                    ordered_and_preselected_recipes_joined.preselected_product_variation_id,
                    order_line_dimensions_joined.company_id
                    )
                ), '0'
            ) as fk_dim_products_preselected
        , coalesce(
            md5(
                cast(
                    concat(
                        ordered_and_preselected_recipes_joined.recipe_id, 
                        order_line_dimensions_joined.language_id
                        ) as string
                    )
                ), '0'
            ) as fk_dim_recipes
        , coalesce(
            md5(
                cast(
                    concat(
                        ordered_and_preselected_recipes_joined.preselected_recipe_id, 
                        order_line_dimensions_joined.language_id
                        ) as string
                    )
                ), '0'
            ) as fk_dim_recipes_preselected
    from order_line_dimensions_joined
    left join ordered_and_preselected_recipes_joined
        on order_line_dimensions_joined.billing_agreement_order_id = ordered_and_preselected_recipes_joined.billing_agreement_order_id
    where ordered_and_preselected_recipes_joined.is_generated_recipe_line = true

)


, add_pk (
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
    from add_recipes_to_orders
)

select * from add_pk