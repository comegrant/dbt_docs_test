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

, recipe_feedback as (

    select * from {{ ref('int_recipe_ratings_comments_joined') }}

)

-- TODO: This solution is a bit hacky
, recommendations_origin as (

    select distinct
        billing_agreement_basket_id
        , menu_week_monday_date
        , billing_agreement_basket_deviation_origin_id
    from recommendations

)

, order_line_dimensions_joined as (

    select
        order_lines.*
        , deviations_order_mapping.is_onesub_migration
        , case when is_onesub_migration = 0
            and menu_week_monday_date >= '{{ var("onesub_full_launch_date") }}'
            and recommendations_origin.billing_agreement_basket_deviation_origin_id is null
            then 1
            else 0
        end as is_missing_preselector_output
        , products.portions
        , products.meals
        , products.product_type_id
        , companies.company_id
        , companies.language_id
        , deviations_order_mapping.billing_agreement_basket_deviation_origin_id
        , recommendations_origin.billing_agreement_basket_deviation_origin_id as billing_agreement_basket_deviation_origin_id_preselected
        , billing_agreements_ordergen.pk_dim_billing_agreements as fk_dim_billing_agreements_ordergen
        , coalesce(billing_agreements_deviations.pk_dim_billing_agreements, billing_agreements_ordergen.pk_dim_billing_agreements) as fk_dim_billing_agreements_deviations
        , coalesce(md5(deviations_order_mapping.billing_agreement_basket_deviation_origin_id), md5('00000000-0000-0000-0000-000000000000')) as fk_dim_basket_deviation_origins
        , coalesce(md5(recommendations_origin.billing_agreement_basket_deviation_origin_id), md5('00000000-0000-0000-0000-000000000000')) as fk_dim_basket_deviation_origins_preselected
        , companies.pk_dim_companies as fk_dim_companies
        , cast(date_format(order_lines.menu_week_monday_date, 'yyyyMMdd') as int) as fk_dim_date
        , md5(order_lines.order_status_id) AS fk_dim_order_statuses
        , md5(order_lines.order_type_id) AS fk_dim_order_types
        , coalesce(products.pk_dim_products, 0) AS fk_dim_products
    from order_lines
    left join deviations_order_mapping
        on order_lines.billing_agreement_order_id = deviations_order_mapping.billing_agreement_order_id
    left join recommendations_origin
        on deviations_order_mapping.billing_agreement_basket_id = recommendations_origin.billing_agreement_basket_id
        and deviations_order_mapping.menu_week_monday_date = recommendations_origin.menu_week_monday_date
    left join billing_agreements as billing_agreements_ordergen
        on order_lines.billing_agreement_id = billing_agreements_ordergen.billing_agreement_id
        and order_lines.source_created_at >= billing_agreements_ordergen.valid_from
        and order_lines.source_created_at < billing_agreements_ordergen.valid_to
    left join billing_agreements as billing_agreements_deviations
        on order_lines.billing_agreement_id = billing_agreements_deviations.billing_agreement_id
        and deviations_order_mapping.billing_agreement_valid_at >= billing_agreements_deviations.valid_from
        and deviations_order_mapping.billing_agreement_valid_at  < billing_agreements_deviations.valid_to
    left join products
        on order_lines.product_variation_id = products.product_variation_id
        and billing_agreements_ordergen.company_id = products.company_id
    left join companies
        on billing_agreements_ordergen.company_id = companies.company_id

)

-- ASSUMPTION (Pre OneSub): Not possible to have a mealbox product and velg&vrak dishes
, ordered_recipes as (

    select distinct
        order_line_dimensions_joined.billing_agreement_order_id
        , order_line_dimensions_joined.billing_agreement_order_line_id
        , order_line_dimensions_joined.product_type_id
        , order_line_dimensions_joined.product_variation_id
        , menus.recipe_id
    from order_line_dimensions_joined
    left join menus
        on order_line_dimensions_joined.menu_week_monday_date = menus.menu_week_monday_date
        and order_line_dimensions_joined.product_variation_id = menus.product_variation_id
        and order_line_dimensions_joined.company_id = menus.company_id
    where menus.recipe_id is not null

)

-- Find the product variations the customer subscribed to when placing the order
, subscribed_product_variations as (

    select distinct
        order_line_dimensions_joined.menu_week_monday_date
        , order_line_dimensions_joined.billing_agreement_id
        , order_line_dimensions_joined.billing_agreement_order_id
        , order_line_dimensions_joined.company_id
        , products.product_type_id as subscribed_product_type_id
        , products.product_id as subscribed_product_id
        , products.product_variation_id as subscribed_product_variation_id
        , bridge_subscribed_products.product_variation_id as subscribed_product_variation_quantity
        , products.meals as subscribed_meals
        , products.portions as subscribed_portions
    from order_line_dimensions_joined
    -- only include billing agreements that has subscribed products
    inner join bridge_subscribed_products
        on order_line_dimensions_joined.fk_dim_billing_agreements_deviations = bridge_subscribed_products.fk_dim_billing_agreements
    left join products
        on bridge_subscribed_products.fk_dim_products = products.pk_dim_products
    -- TODO: Use variable for this
    -- only extract subscription related orders
    where order_line_dimensions_joined.order_type_id in (
        '1C182E51-ECFA-4119-8928-F2D9F57C5FCC', 
        '5F34860B-7E61-46A0-80F7-98DCDC53BA9E', 
        'C7D2684C-B715-4C6C-BF90-053757926679'
    )
    -- TODO: Can be removed after scd2 of basket products are fixed
    and order_line_dimensions_joined.menu_week_monday_date >= '{{ var("mealbox_adjustments_cutoff") }}'

)

-- Find the preselected recipes for each product variation made by the chefs
, chef_preselected_recipes as (

    select distinct
        subscribed_product_variations.billing_agreement_order_id
        , menus.product_variation_id
        , menus.recipe_id
    from subscribed_product_variations
    left join menus
        on subscribed_product_variations.menu_week_monday_date = menus.menu_week_monday_date
        and subscribed_product_variations.subscribed_product_variation_id = menus.product_variation_id
        and subscribed_product_variations.company_id = menus.company_id
    where menus.recipe_id is not null
    and menus.menu_week_monday_date >= '{{ var("mealbox_adjustments_cutoff") }}'
    and subscribed_product_variations.subscribed_product_type_id = '{{ var("mealbox_product_type_id") }}'

)

-- Find the preselected recipes for each product variation made by the recommendation engine
, recommendation_engine_preselected_recipes as (

    select distinct
      deviations_order_mapping.billing_agreement_order_id
      , recommendations.product_variation_id
      , menus.recipe_id
    from deviations_order_mapping
    left join recommendations
        on deviations_order_mapping.billing_agreement_basket_id = recommendations.billing_agreement_basket_id
        and deviations_order_mapping.menu_week_monday_date = recommendations.menu_week_monday_date
    left join menus
        on recommendations.menu_week_monday_date = menus.menu_week_monday_date
        and recommendations.product_variation_id = menus.product_variation_id
        and deviations_order_mapping.company_id = menus.company_id
    left join products
        on recommendations.product_variation_id = products.product_variation_id
        and deviations_order_mapping.company_id = products.company_id
    where menus.recipe_id is not null
    and products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
    and menus.menu_week_monday_date >= '{{ var("mealbox_adjustments_cutoff") }}'

)

, preselected_recipes_unioned (

    select * from recommendation_engine_preselected_recipes

    union all
    
    select 
        chef_preselected_recipes.* 
    from chef_preselected_recipes
    left join recommendation_engine_preselected_recipes 
        on chef_preselected_recipes.billing_agreement_order_id = recommendation_engine_preselected_recipes.billing_agreement_order_id
    where recommendation_engine_preselected_recipes.billing_agreement_order_id is null

)

, ordered_and_preselected_recipes_joined as (

    select
        coalesce(
            ordered_recipes.billing_agreement_order_id
            , preselected_recipes_unioned.billing_agreement_order_id
        ) as billing_agreement_order_id
        , ordered_recipes.billing_agreement_order_line_id
        , ordered_recipes.recipe_id
        , preselected_recipes_unioned.recipe_id as preselected_recipe_id
        , ordered_recipes.product_variation_id
        , preselected_recipes_unioned.product_variation_id as preselected_product_variation_id
    from ordered_recipes
    full join preselected_recipes_unioned
        on ordered_recipes.billing_agreement_order_id = preselected_recipes_unioned.billing_agreement_order_id
        and ordered_recipes.recipe_id = preselected_recipes_unioned.recipe_id

)

, add_recipes_to_orders as (
    
    -- Add Velg&Vrak recipes that have not been changed
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
        , case 
            when ordered_and_preselected_recipes_joined.recipe_id = ordered_and_preselected_recipes_joined.preselected_recipe_id
            then 0
            when order_line_dimensions_joined.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
            and ordered_and_preselected_recipes_joined.preselected_recipe_id is null
            and order_line_dimensions_joined.menu_week_monday_date >= '{{ var("mealbox_adjustments_cutoff") }}'
            then 1
            else null
        end as is_added_dish
        -- this part of the union will not consist of any removed dishes
        , case 
            when order_line_dimensions_joined.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
            and order_line_dimensions_joined.menu_week_monday_date >= '{{ var("mealbox_adjustments_cutoff") }}'
            then 0
            else null
        end as is_removed_dish
        , case 
            when order_line_dimensions_joined.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
            then true
            else false
        end as is_dish
        , case
            when order_line_dimensions_joined.product_type_id = '{{ var("mealbox_product_type_id") }}' 
            or order_line_dimensions_joined.product_type_id = '{{ var("financial_product_type_id") }}'
            then order_line_dimensions_joined.meals - subscribed_mealbox.subscribed_meals
            else null
        end as meal_adjustment_subscription
        , case 
            when order_line_dimensions_joined.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
            then order_line_dimensions_joined.portions - subscribed_mealbox.subscribed_portions
            else null
        end as portion_adjustment_subscription
        , order_line_dimensions_joined.order_line_type_name
        , ordered_and_preselected_recipes_joined.recipe_id
        , ordered_and_preselected_recipes_joined.preselected_recipe_id
        , order_line_dimensions_joined.has_delivery
        , order_line_dimensions_joined.has_recipe_leaflets
        , order_line_dimensions_joined.is_onesub_migration
        , order_line_dimensions_joined.is_missing_preselector_output
        , order_line_dimensions_joined.billing_agreement_basket_deviation_origin_id
        , order_line_dimensions_joined.billing_agreement_basket_deviation_origin_id_preselected
        , order_line_dimensions_joined.billing_agreement_id
        , order_line_dimensions_joined.company_id
        , order_line_dimensions_joined.language_id
        , order_line_dimensions_joined.order_status_id
        , order_line_dimensions_joined.order_type_id
        , order_line_dimensions_joined.product_variation_id
        , ordered_and_preselected_recipes_joined.preselected_product_variation_id
        , order_line_dimensions_joined.fk_dim_basket_deviation_origins
        , order_line_dimensions_joined.fk_dim_basket_deviation_origins_preselected
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
        and order_line_dimensions_joined.product_type_id != '{{ var("mealbox_product_type_id") }}'
        and ordered_and_preselected_recipes_joined.billing_agreement_order_line_id is not null
    -- ASSUMPTION: A customer can only have one subscribed mealbox product
    left join subscribed_product_variations as subscribed_mealbox
        on order_line_dimensions_joined.billing_agreement_order_id = subscribed_mealbox.billing_agreement_order_id
        and subscribed_mealbox.subscribed_product_type_id = '{{ var("mealbox_product_type_id") }}'
    
        
    union all

    -- Add velg&vrak recipes that was removed
    select distinct
        order_line_dimensions_joined.menu_year
        , order_line_dimensions_joined.menu_week
        , order_line_dimensions_joined.menu_week_monday_date
        , order_line_dimensions_joined.source_created_at
        , order_line_dimensions_joined.billing_agreement_order_id
        , order_line_dimensions_joined.ops_order_id
        , ordered_and_preselected_recipes_joined.billing_agreement_order_line_id
        , 0 as product_variation_quantity
        , 0 as vat
        , 0 as unit_price_ex_vat
        , 0 as unit_price_inc_vat
        , 0 as total_amount_ex_vat
        , 0 as total_amount_inc_vat
        , 0 as is_added_dish
        , 1 as is_removed_dish
        , true as is_dish
        , null as meal_adjustment_subscription
        -- Portion adjustments are only relevant for added recipes
        , null as portion_adjustment_subscription
        , "GENERATED" as order_line_type_name
        , ordered_and_preselected_recipes_joined.recipe_id
        , ordered_and_preselected_recipes_joined.preselected_recipe_id
        , order_line_dimensions_joined.has_delivery
        , order_line_dimensions_joined.has_recipe_leaflets
        , order_line_dimensions_joined.is_onesub_migration
        , order_line_dimensions_joined.is_missing_preselector_output
        , order_line_dimensions_joined.billing_agreement_basket_deviation_origin_id
        , order_line_dimensions_joined.billing_agreement_basket_deviation_origin_id_preselected
        , order_line_dimensions_joined.billing_agreement_id
        , order_line_dimensions_joined.company_id
        , order_line_dimensions_joined.language_id
        , order_line_dimensions_joined.order_status_id
        , order_line_dimensions_joined.order_type_id
        , null as product_variation_id
        , ordered_and_preselected_recipes_joined.preselected_product_variation_id
        , order_line_dimensions_joined.fk_dim_basket_deviation_origins
        , order_line_dimensions_joined.fk_dim_basket_deviation_origins_preselected
        , order_line_dimensions_joined.fk_dim_billing_agreements_ordergen
        , order_line_dimensions_joined.fk_dim_billing_agreements_deviations
        , order_line_dimensions_joined.fk_dim_companies
        , order_line_dimensions_joined.fk_dim_date
        , order_line_dimensions_joined.fk_dim_order_statuses
        , order_line_dimensions_joined.fk_dim_order_types
        , 0 as fk_dim_products
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
    from ordered_and_preselected_recipes_joined
    left join order_line_dimensions_joined
        on ordered_and_preselected_recipes_joined.billing_agreement_order_id = order_line_dimensions_joined.billing_agreement_order_id
    where ordered_and_preselected_recipes_joined.billing_agreement_order_line_id is null

    union all 

    -- (Legacy) Add recipes that belong to mealbox products before Onesub
    select distinct
        order_line_dimensions_joined.menu_year
        , order_line_dimensions_joined.menu_week
        , order_line_dimensions_joined.menu_week_monday_date
        , order_line_dimensions_joined.source_created_at
        , order_line_dimensions_joined.billing_agreement_order_id
        , order_line_dimensions_joined.ops_order_id
        , ordered_and_preselected_recipes_joined.billing_agreement_order_line_id
        , 0 as product_variation_quantity
        , 0 as vat
        , 0 as unit_price_ex_vat
        , 0 as unit_price_inc_vat
        , 0 as total_amount_ex_vat
        , 0 as total_amount_inc_vat
        , case 
            when ordered_and_preselected_recipes_joined.preselected_recipe_id = ordered_and_preselected_recipes_joined.recipe_id
            then 0
            when ordered_and_preselected_recipes_joined.preselected_recipe_id is null
            and ordered_and_preselected_recipes_joined.recipe_id is not null
            and order_line_dimensions_joined.menu_week_monday_date >= '{{ var("mealbox_adjustments_cutoff") }}'
            then 1
            else null
        end as is_added_dish
        , case 
            when ordered_and_preselected_recipes_joined.preselected_recipe_id = ordered_and_preselected_recipes_joined.recipe_id
            then 0
            when ordered_and_preselected_recipes_joined.preselected_recipe_id is not null
            and ordered_and_preselected_recipes_joined.recipe_id is null
            and order_line_dimensions_joined.menu_week_monday_date >= '{{ var("mealbox_adjustments_cutoff") }}'
            then 1
            else null
        end as is_removed_dish
        , true as is_dish
        , null as meal_adjustment_subscription
        , order_line_dimensions_joined.portions - subscribed_mealbox.subscribed_portions as portion_adjustment_subscription
        , "GENERATED" as order_line_type_name
        , ordered_and_preselected_recipes_joined.recipe_id
        , ordered_and_preselected_recipes_joined.preselected_recipe_id
        , order_line_dimensions_joined.has_delivery
        , order_line_dimensions_joined.has_recipe_leaflets
        , order_line_dimensions_joined.billing_agreement_basket_deviation_origin_id
        , order_line_dimensions_joined.billing_agreement_basket_deviation_origin_id_preselected
        , order_line_dimensions_joined.is_onesub_migration
        , order_line_dimensions_joined.is_missing_preselector_output
        , order_line_dimensions_joined.billing_agreement_id
        , order_line_dimensions_joined.company_id
        , order_line_dimensions_joined.language_id
        , order_line_dimensions_joined.order_status_id
        , order_line_dimensions_joined.order_type_id
        , order_line_dimensions_joined.product_variation_id
        , ordered_and_preselected_recipes_joined.preselected_product_variation_id
        , order_line_dimensions_joined.fk_dim_basket_deviation_origins
        , order_line_dimensions_joined.fk_dim_basket_deviation_origins_preselected
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
    from ordered_and_preselected_recipes_joined
    left join order_line_dimensions_joined
        on ordered_and_preselected_recipes_joined.billing_agreement_order_line_id = order_line_dimensions_joined.billing_agreement_order_line_id
    -- ASSUMPTION: A customer can only have one subscribed mealbox product
    left join subscribed_product_variations as subscribed_mealbox
        on order_line_dimensions_joined.billing_agreement_order_id = subscribed_mealbox.billing_agreement_order_id
        and subscribed_mealbox.subscribed_product_type_id = '{{ var("mealbox_product_type_id") }}'
    where order_line_dimensions_joined.product_type_id = '{{ var("mealbox_product_type_id") }}'

)

, add_recipe_feedback as (
    select 
        add_recipes_to_orders.*
        , recipe_feedback.recipe_rating_id
        , recipe_feedback.recipe_comment_id
        , recipe_feedback.recipe_rating
        , recipe_feedback.recipe_rating_score
        , recipe_feedback.is_not_cooked_dish
        , recipe_feedback.recipe_comment
        , greatest(
            add_recipes_to_orders.source_created_at
            , recipe_feedback.source_updated_at
        ) as source_updated_at
    from add_recipes_to_orders
    left join recipe_feedback
        on add_recipes_to_orders.recipe_id = recipe_feedback.recipe_id
        and add_recipes_to_orders.billing_agreement_id = recipe_feedback.billing_agreement_id


)

, add_pk as (
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
        , add_recipe_feedback.* 
    from add_recipe_feedback
)

select * from add_pk