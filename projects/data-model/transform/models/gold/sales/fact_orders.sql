with

-- SOURCES
-- Silver
discounts as (

    select * from {{ ref('cms__billing_agreement_order_discounts') }}

)

, orders as (

    select * from {{ ref('cms__billing_agreement_orders') }}

)

, baskets as (

    select * from {{ref('cms__billing_agreement_baskets')}}
    where valid_to = '{{ var("future_proof_date") }}'

)

-- Intermediate
, deviation_products as (

    select * from {{ ref('int_basket_deviation_products_joined') }}

)

, deviations_aggregated as (

    select * from {{ ref('int_basket_deviation_products_aggregated') }}

)

, menus as (

    select * from {{ ref('int_weekly_menus_variations_recipes_portions_joined') }}

)

, order_lines as (

    select * from {{ ref('int_billing_agreement_order_lines_joined') }}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, recipe_feedback as (

    select * from {{ ref('int_recipe_ratings_comments_joined') }}

)

, subscription_products as (

    select * from {{ ref('int_subscribed_products_scd2') }}

)

, customer_journey_segments as (

    select * from {{ ref('int_customer_journey_segments') }}
)

, recipe_costs_and_co2 as (

    select * from {{ ref('int_weekly_recipe_costs_and_co2')}}

)

-- Gold
, billing_agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, companies as (

    select * from {{ ref('dim_companies') }}

)

, portions as (

    select * from {{ ref('dim_portions') }}

)

, loyalty_seasons as (

    select * from {{ ref('dim_loyalty_seasons') }}

)

-- FIND SUBCRIPTION ORDERS
-- Assumptions:
-- 1) Only one subscription order per customer per menu week
, subscription_orders as (

    select distinct
        orders.billing_agreement_order_id
        , orders.menu_week_monday_date
        , orders.billing_agreement_id
        , baskets.billing_agreement_basket_id
        , baskets.basket_type_id
        , orders.source_created_at
        , deviations_aggregated.order_placed_at
        , max(
                case 
                when baskets.basket_type_id = '{{ var("grocery_basket_type_id") }}' 
                then true 
                else false 
                end
            ) 
            over (
                partition by 
                    orders.billing_agreement_id
                    , orders.menu_week_monday_date
                ) as has_grocery_basket
        , coalesce(deviations_aggregated.order_placed_at, orders.source_created_at) as billing_agreements_join_timestamp
        , coalesce(deviations_aggregated.has_recommendation, false) as has_recommendation
        , coalesce(deviations_aggregated.is_onesub_migration, 0) as is_onesub_migration
        , coalesce(deviations_aggregated.billing_agreement_basket_id is not null, false ) as has_valid_deviation
    from orders

    left join baskets
        on orders.billing_agreement_id = baskets.billing_agreement_id
        and orders.source_created_at >= baskets.valid_from
        and orders.source_created_at < baskets.valid_to
    
    left join deviations_aggregated
        on baskets.billing_agreement_basket_id = deviations_aggregated.billing_agreement_basket_id
        and orders.menu_week_monday_date = deviations_aggregated.menu_week_monday_date

    -- only include orders that are a part of a subscription
    where orders.order_type_id in ({{var ('subscription_order_type_ids') | join(', ')}})
    
    -- only include orders that occured after the cutoff for when we analyse mealbox adjustments
    and orders.menu_week_monday_date >= '{{ var("mealbox_adjustments_cutoff") }}'

)

-- FIND SUBSCRIBED PRODUCTS AND RECIPES

-- Find products outputted by the preselector
, subscription_orders_map_recommended_products as (

    select
        subscription_orders.menu_week_monday_date
        , subscription_orders.billing_agreement_order_id
        , subscription_orders.billing_agreement_id
        , deviation_products.company_id
        , subscription_orders.billing_agreement_basket_id
        , deviation_products.product_variation_id
        , deviation_products.product_variation_quantity
        , subscription_orders.has_recommendation
        , subscription_orders.is_onesub_migration
        , case 
            -- TODO: A bit unsure if I should do this
            when subscription_orders.is_onesub_migration = 1 
            then '0' 
            else deviation_products.billing_agreement_basket_deviation_origin_id 
            end as billing_agreement_basket_deviation_origin_id
    from subscription_orders
    
    left join deviation_products
        on subscription_orders.menu_week_monday_date = deviation_products.menu_week_monday_date
        and subscription_orders.billing_agreement_basket_id = deviation_products.billing_agreement_basket_id
        -- only include the last deviation created by the preselector/mealselector
        and deviation_products.recommendation_version_desc = 1
    
    left join products
        on deviation_products.product_variation_id = products.product_variation_id
        and deviation_products.company_id = products.company_id
    
    -- exclude orders with no active deviations
    where subscription_orders.has_valid_deviation is true

    -- remove baskets with no deviation product variations
    and deviation_products.product_variation_id is not null

    -- only include velg&vrak products
    and products.product_type_id in (
        '{{ var("velg&vrak_product_type_id") }}'
    )

    -- TEMP: Remove when CMS has made fix on their side
    -- Need to do this since CMS by mistake have added
    -- preselector output to the grocery basket during the migration
    -- of grocery baskets
    and subscription_orders.basket_type_id = '{{ var("mealbox_basket_type_id") }}'

)

-- Find products in the customers subscription
, subscription_orders_map_subscription_products as (

    select 
        subscription_orders.menu_week_monday_date
        , subscription_orders.billing_agreement_order_id
        , subscription_orders.billing_agreement_id
        , subscription_products.company_id
        , subscription_orders.billing_agreement_basket_id
        , subscription_products.product_variation_id
        , subscription_products.product_variation_quantity
        , subscription_orders.has_recommendation
        , subscription_orders.is_onesub_migration
        , '0' as billing_agreement_basket_deviation_origin_id
    from subscription_orders

    left join subscription_products
        on subscription_orders.billing_agreement_basket_id = subscription_products.billing_agreement_basket_id
        and subscription_orders.billing_agreements_join_timestamp >= subscription_products.valid_from
        and subscription_orders.billing_agreements_join_timestamp < subscription_products.valid_to

    left join products
        on subscription_products.product_variation_id = products.product_variation_id
        and subscription_products.company_id = products.company_id
    
    -- remove groceries from mealbox basket if grocery basket exists
    -- since the billing_agreement_join_timestamp can be different for grocery baskets
    -- and mealbox baskets it can result in overlapping grocery products around the
    -- implementation of the basket split (May 2025)
    where not (
        subscription_orders.has_grocery_basket is true
        and subscription_orders.basket_type_id = '{{ var("mealbox_basket_type_id") }}'
        and products.product_type_id != '{{ var("mealbox_product_type_id") }}'

    )

    -- remove baskets with no subscribed product variations
    and subscription_products.product_variation_id is not null

)

, recommended_and_subscription_products_unioned as (

    select * from subscription_orders_map_recommended_products

    union all 

    select * from subscription_orders_map_subscription_products

)

-- Find the recipes that related to the product variations the customers
-- would have received if not making any changes to their order
, recommended_and_subscription_products_unioned_add_recipes as (

    -- Find recipes related to product variations which only has one recipe related to it
    -- Mainly relevant for groceries and velg&vrak-dishes selected by the preselector/mealselector
    -- For products with more than one recipe related to it, we want to keep the product variation without the recipes
    -- and then add the recipes as new rows. This is to make the join with orders later on correct.
    select 
        recommended_and_subscription_products_unioned.*
        , menus.recipe_id
        , coalesce(menus.recipe_id, recommended_and_subscription_products_unioned.product_variation_id) as orders_subscriptions_match_key
    from recommended_and_subscription_products_unioned 
    left join menus
        on recommended_and_subscription_products_unioned.menu_week_monday_date = menus.menu_week_monday_date
        and recommended_and_subscription_products_unioned.product_variation_id = menus.product_variation_id
        and recommended_and_subscription_products_unioned.company_id = menus.company_id
        and menus.menu_number_days = 1
    
    union all

    -- Find recipes related to product variations that has more than 1 recipe related to it
    -- Mainly relevant for mealboxes before Onesub was launched (October 2024),
    -- but could also happen for products that are not mealboxes nor dishes, such as add-ons.
    select 
        recommended_and_subscription_products_unioned.*
        , menus.recipe_id
        , menus.recipe_id as orders_subscriptions_match_key
    from recommended_and_subscription_products_unioned 
    left join menus
        on recommended_and_subscription_products_unioned.menu_week_monday_date = menus.menu_week_monday_date
        and recommended_and_subscription_products_unioned.product_variation_id = menus.product_variation_id
        and recommended_and_subscription_products_unioned.company_id = menus.company_id
    -- Add recipes to mealboxes (before onesub) or other products that relates to more than one dish at once
    -- for example campaign mealboxes such as julekassen and påskekassen
    where menus.menu_number_days > 1
    -- Its only relevant to fetch the rows if there exist related recipes
    and menus.recipe_id is not null
    -- Some customers had recommendations before Onesub if they had special meal preferneces (e.g. no pork)
    -- hence we exclude recipes related to the subscribed mealbox (pre Onesub) if recommendations exist
    and not (recommended_and_subscription_products_unioned.has_recommendation and menus.is_mealbox)

)

-- ADD RECIPES TO ORDERS

-- for many products order lines and recipes are 1:1
-- in these cases the recipes are added to the same row as the order line
, order_lines_add_one_meal_recipes as (

    select
        
        order_lines.company_id
        , order_lines.billing_agreement_order_id
        , order_lines.product_variation_id
        , menus.recipe_id
        , menus.recipe_portion_id
        , coalesce(menus.recipe_id, order_lines.product_variation_id) as orders_subscriptions_match_key
        , order_lines.billing_agreement_order_line_id
        , order_lines.product_variation_quantity
        , order_lines.vat
        , order_lines.unit_price_ex_vat
        , order_lines.unit_price_inc_vat
        , order_lines.total_amount_ex_vat
        , order_lines.total_amount_inc_vat
        , order_lines.order_line_type_name
    from order_lines
    
    left join menus
        on order_lines.menu_week_monday_date = menus.menu_week_monday_date
        and order_lines.product_variation_id = menus.product_variation_id
        and order_lines.company_id = menus.company_id
        and menus.menu_number_days = 1
    
)


-- Some product variations has several recipes, but only one order line
-- in these cases the recipes are added as new rows.
-- Example: This mainly applies to mealboxes ordered before Onesub was launched
-- were the customer did not make any changes to their order
-- however it also include campaign mealboxes for christmas or easter
, order_lines_add_multi_meal_recipes as (

    select
        order_lines.company_id
        , order_lines.billing_agreement_order_id
        , order_lines.product_variation_id
        , menus.recipe_id
        , menus.recipe_portion_id
        , menus.recipe_id as orders_subscriptions_match_key
        , order_lines.billing_agreement_order_line_id
        , {{ generate_null_columns(6, prefix='null_col') }}
        , 'GENERATED' as order_line_type_name
    from order_lines
    
    left join menus
        on order_lines.menu_week_monday_date = menus.menu_week_monday_date
        and order_lines.product_variation_id = menus.product_variation_id
        and order_lines.company_id = menus.company_id
    
    where menus.menu_number_days > 1
    -- ASSUMPTION: Must have recipes to be a valid product with more than 1 menu days
    and menus.recipe_id is not null

)

, order_lines_one_meal_and_multi_meal_recipes_unioned as (
   
    select * from order_lines_add_one_meal_recipes
    
    union all

    select * from order_lines_add_multi_meal_recipes

)

-- ADD SUBSCRIBED PRODUCTS AND RECIPES TO ORDERS
, ordered_subscription_and_recommended_recipes_joined as (

    select
        coalesce(
                order_lines_one_meal_and_multi_meal_recipes_unioned.billing_agreement_order_id
                , recommended_and_subscription_products_unioned_add_recipes.billing_agreement_order_id
            ) as billing_agreement_order_id
        , coalesce(
                order_lines_one_meal_and_multi_meal_recipes_unioned.company_id
                , recommended_and_subscription_products_unioned_add_recipes.company_id
            ) as company_id
        , order_lines_one_meal_and_multi_meal_recipes_unioned.* except(billing_agreement_order_id, company_id, order_line_type_name)
        , coalesce(
                order_lines_one_meal_and_multi_meal_recipes_unioned.order_line_type_name
                , 'GENERATED'
        ) as order_line_type_name
        , recommended_and_subscription_products_unioned_add_recipes.product_variation_id as product_variation_id_subscription
        , recommended_and_subscription_products_unioned_add_recipes.recipe_id as recipe_id_subscription
        , recommended_and_subscription_products_unioned_add_recipes.product_variation_quantity as product_variation_quantity_subscription
        , recommended_and_subscription_products_unioned_add_recipes.product_variation_quantity * order_lines_one_meal_and_multi_meal_recipes_unioned.unit_price_ex_vat as total_amount_ex_vat_subscription
        , recommended_and_subscription_products_unioned_add_recipes.billing_agreement_basket_deviation_origin_id
        , recommended_and_subscription_products_unioned_add_recipes.is_onesub_migration
    from order_lines_one_meal_and_multi_meal_recipes_unioned
    
    full join recommended_and_subscription_products_unioned_add_recipes
        on order_lines_one_meal_and_multi_meal_recipes_unioned.billing_agreement_order_id = recommended_and_subscription_products_unioned_add_recipes.billing_agreement_order_id
        and order_lines_one_meal_and_multi_meal_recipes_unioned.orders_subscriptions_match_key = recommended_and_subscription_products_unioned_add_recipes.orders_subscriptions_match_key

)

-- ADD COLUMNS
, add_order_level_columns as (

    select
        orders.*
        , ordered_subscription_and_recommended_recipes_joined.* except(billing_agreement_order_id)
        , companies.language_id
        -- TODO: This should be added in silver instead
        , {{ get_financial_date_from_monday_date('orders.menu_week_monday_date') }} as menu_week_financial_date

        , case
            when
                orders.order_type_id = '{{var ("daily_direct_order_type_id")}}'
                or orders.order_type_id = '{{var ("campaign_order_type_id")}}'
                or orders.order_type_id in ({{var ('subscription_order_type_ids') | join(', ')}})
            then true
            else false
        end as has_normal_order_type

        , case 
            when orders.order_type_id in ({{var ('subscription_order_type_ids') | join(', ')}})
            then true
            else false
        end as has_subscription_order_type

        , case 
            when orders.order_status_id in ({{var ('finished_order_status_ids') | join(', ')}})
            then true
            else false
        end as has_finished_order_status

    from ordered_subscription_and_recommended_recipes_joined
    left join orders
        on ordered_subscription_and_recommended_recipes_joined.billing_agreement_order_id = orders.billing_agreement_order_id
    left join companies
        on ordered_subscription_and_recommended_recipes_joined.company_id = companies.company_id

)

, add_product_columns as (
    select    
        add_order_level_columns.*

        -- order lines information
        , case
            when products.product_variation_id = 'B8F600F3-DEE0-4762-BAE6-5A501B9C1FA3'
            then 'Campaign Discount'
            when products.product_variation_id = '40511016-FE46-4DC8-88A8-B4DF3A180CBA'
            then 'Discount'
            when products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
            and add_order_level_columns.total_amount_ex_vat > 0
            then 'Plus Price Dish'
            when products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
            and add_order_level_columns.total_amount_ex_vat < 0
            then 'Thrifty Dish'
            when products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
            and add_order_level_columns.total_amount_ex_vat = 0
            then 'Normal Dish'
            when products.product_type_id = '{{ var("mealbox_product_type_id") }}'
            and add_order_level_columns.total_amount_ex_vat is null
            then 'Normal Dish'
            when products.product_type_id in (
                '{{ var("mealbox_product_type_id") }}',
                '{{ var("financial_product_type_id") }}'
                )
            then 'Mealbox'
            when products.product_type_id in ({{ var('grocery_product_type_ids') | join(', ') }})
            then 'Groceries'
        else add_order_level_columns.order_line_type_name
        end as order_line_details

        -- meals and portions
        , case 
            -- TODO: A bit unsure if this make sense
            -- set meals to one for dishes of the mealbox product
            when
                products.product_type_id = '{{ var("mealbox_product_type_id") }}' 
                and add_order_level_columns.recipe_id is not null
            then 1
            else products.meals
            end as meals
        , subscription_products.meals as meals_subscription
        , portions.portions
        , portions_subscription.portions as portions_subscription
        , portions.portion_id
        , portions_subscription.portion_id as portion_id_subscription
        , case 
            when
                products.product_type_id in ('{{ var("mealbox_product_type_id") }}', '{{ var("financial_product_type_id") }}')
                -- TODO: not super robust - if a mealbox has just one recipe the whole mealbox will be excluded
                -- since it will not have several rows
                -- also opposite problem, if meals = 1 and recipe id does not exist it will get mealbox servings
                and add_order_level_columns.recipe_id is null
                and add_order_level_columns.has_subscription_order_type is true
            then products.meals * portions.portions
            else null
            end as mealbox_servings
    
        , case 
            when
                subscription_products.product_type_id in ('{{ var("mealbox_product_type_id") }}', '{{ var("financial_product_type_id") }}')
                -- TODO: not super robust - if a mealbox has just one recipe the whole mealbox will be excluded
                -- since it will not have several rows
                -- also opposite problem, if meals = 1 and recipe id does not exist it will get mealbox servings
                and add_order_level_columns.recipe_id_subscription is null
                and add_order_level_columns.has_subscription_order_type is true
            then subscription_products.meals * portions_subscription.portions
            else null
            end as mealbox_servings_subscription

        -- mealbox and dishes
        , case 
            when
                products.product_type_id in ('{{ var("mealbox_product_type_id") }}', '{{ var("financial_product_type_id") }}')
                and add_order_level_columns.recipe_id is null
                -- TODO: Should this include campaign mealboxes as well?
                and add_order_level_columns.has_subscription_order_type is true
            then true
            else false
        end as is_mealbox

        , case 
            when
                subscription_products.product_type_id = '{{ var("mealbox_product_type_id") }}'
                and add_order_level_columns.recipe_id_subscription is null
                and add_order_level_columns.has_subscription_order_type is true
            then true
            else false
        end as is_subscribed_mealbox

        , case 
            when products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
            or (
                products.product_type_id = '{{ var("mealbox_product_type_id") }}' 
                and add_order_level_columns.recipe_id is not null
                -- TODO: Should this flag work for campaign orders?
                and add_order_level_columns.has_subscription_order_type is true
            )
            then true
            else false
        end as is_dish

        , case 
            when subscription_products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
            or (
                subscription_products.product_type_id = '{{ var("mealbox_product_type_id") }}' 
                and add_order_level_columns.recipe_id_subscription is not null
                and add_order_level_columns.has_subscription_order_type is true
            )
            then true
            else false
        end as is_preselected_dish

        , case 
            when products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
            and add_order_level_columns.total_amount_ex_vat > 0
            then 1
            when products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
            then 0
            when products.product_type_id = '{{ var("mealbox_product_type_id") }}'
            and add_order_level_columns.total_amount_ex_vat is null
            then 0
            else null
        end as is_plus_price_dish

        , case 
            when products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
            and add_order_level_columns.total_amount_ex_vat < 0
            then 1
            when products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
            then 0
            when products.product_type_id = '{{ var("mealbox_product_type_id") }}'
            and add_order_level_columns.total_amount_ex_vat is null
            then 0
            else null
        end as is_thrifty_dish

        , case 
            when add_order_level_columns.menu_week_monday_date < '{{ var("mealbox_adjustments_cutoff") }}'
            or add_order_level_columns.has_subscription_order_type is false
            then null

            when
                add_order_level_columns.recipe_id is not null 
                and add_order_level_columns.recipe_id_subscription is null
                and (
                    products.product_type_id in ('{{ var("velg&vrak_product_type_id") }}', '{{ var("mealbox_product_type_id") }}') 
                    )
            then 1
            
            when 
                add_order_level_columns.recipe_id is null 
                and add_order_level_columns.recipe_id_subscription is not null
                and (
                    subscription_products.product_type_id in ('{{ var("velg&vrak_product_type_id") }}', '{{ var("mealbox_product_type_id") }}') 
                    )
            then 0

            when
                add_order_level_columns.recipe_id = add_order_level_columns.recipe_id_subscription
                and (
                    products.product_type_id in ('{{ var("velg&vrak_product_type_id") }}', '{{ var("mealbox_product_type_id") }}') 
                    )
            then 0
            
            else null
        end as is_added_dish

        , case
            when add_order_level_columns.menu_week_monday_date < '{{ var("mealbox_adjustments_cutoff") }}'
            or add_order_level_columns.has_subscription_order_type is false
            then null

            when 
                add_order_level_columns.recipe_id is null 
                and add_order_level_columns.recipe_id_subscription is not null
                and (
                    subscription_products.product_type_id in ('{{ var("velg&vrak_product_type_id") }}', '{{ var("mealbox_product_type_id") }}') 
                    )
            then 1

            when
                add_order_level_columns.recipe_id is not null 
                and add_order_level_columns.recipe_id_subscription is null
                and (
                    products.product_type_id in ('{{ var("velg&vrak_product_type_id") }}', '{{ var("mealbox_product_type_id") }}') 
                    )
            then 0

            when 
                add_order_level_columns.recipe_id = add_order_level_columns.recipe_id_subscription
                and (
                    subscription_products.product_type_id in ('{{ var("velg&vrak_product_type_id") }}', '{{ var("mealbox_product_type_id") }}')
                    )
            then 0

            else null
        end as is_removed_dish

        --- groceries
        , case 
            when products.product_type_id in ({{ var('grocery_product_type_ids') | join(', ') }})
            then true
            else false
        end as is_grocery

        , case
            
            when add_order_level_columns.menu_week_monday_date < '{{ var("mealbox_adjustments_cutoff") }}'
            or add_order_level_columns.has_subscription_order_type is false
            then null           

            -- TODO: How to handle subscribed groceries that are not ordered
            when subscription_products.product_type_id in ({{ var('grocery_product_type_ids') | join(', ') }})
            then true
            
            else false
        
        end as is_subscribed_grocery

    from add_order_level_columns
    left join products
        on add_order_level_columns.product_variation_id = products.product_variation_id
        and add_order_level_columns.company_id = products.company_id
    left join products as subscription_products
        on add_order_level_columns.product_variation_id_subscription = subscription_products.product_variation_id
        and add_order_level_columns.company_id = subscription_products.company_id
    left join portions
        on products.portion_name = portions.portion_name_local
        and add_order_level_columns.language_id = portions.language_id
    left join portions as portions_subscription
        on subscription_products.portion_name = portions_subscription.portion_name_local
        and add_order_level_columns.language_id = portions_subscription.language_id

)

, find_swap_information as (

    select

        billing_agreement_order_id
        , sum(is_added_dish) as sum_added_dish
        , sum(is_removed_dish) as sum_removed_dish
        -- TODO: What is the best way to handle this when a customer has several mealboxes on their order?
        , max(
            case 
            when is_mealbox = true
            then meals
            end
        ) as meals_mealbox
        , max(
            case 
            when is_subscribed_mealbox = true
            then meals_subscription
            end
        ) as meals_mealbox_subscription
        , max(
            case 
            when is_mealbox = true
            then portion_id
            end
        ) as mealbox_portions
        , max(
            case 
            when is_mealbox = true
            then portion_id
            end
        ) as portion_id_mealbox
        , max(
            case 
            when is_subscribed_mealbox = true
            then portions_subscription
            end
        ) as portions_mealbox_subscription
        , max(
            case 
            when is_subscribed_mealbox = true
            then portion_id_subscription
            end
        ) as portion_id_mealbox_subscription
        , max(
            case 
                when billing_agreement_basket_deviation_origin_id = '{{ var("preselector_origin_id") }}'
                then true
                else false
                end
        ) as has_preselector_output
        , max(is_onesub_migration) as is_onesub_migration

    from add_product_columns
    where has_subscription_order_type is true
    and menu_week_monday_date >= '{{ var("mealbox_adjustments_cutoff") }}'
    group by 1

)

, add_swap_information as (

    select
        add_product_columns.*
        , find_swap_information.sum_added_dish
        , find_swap_information.sum_removed_dish
        , find_swap_information.meals_mealbox
        , find_swap_information.mealbox_portions
        , find_swap_information.portion_id_mealbox
        , find_swap_information.meals_mealbox_subscription
        , find_swap_information.portions_mealbox_subscription
        , find_swap_information.portion_id_mealbox_subscription
        , case 
            when find_swap_information.has_preselector_output is false 
            and find_swap_information.is_onesub_migration is false
            and add_product_columns.menu_week_monday_date >= '{{ var("onesub_full_launch_date") }}'
            then 1
            else 0
        end as is_missing_preselector_output
        , case 
            when add_product_columns.is_mealbox = true 
            then add_product_columns.meals - find_swap_information.meals_mealbox_subscription 
            end as meal_adjustment_subscription
        , case
            when add_product_columns.is_dish = true
            then add_product_columns.portions - find_swap_information.portions_mealbox_subscription 
            end as portion_adjustment_subscription
        , case 
            -- exclude customers that was never presented with preselector output
            when find_swap_information.has_preselector_output is false 
            and find_swap_information.is_onesub_migration is false
            and add_product_columns.menu_week_monday_date >= '{{ var("onesub_full_launch_date") }}'
            then false
            -- separates swaps from meal adjustments (increase/decrease in meals and not swapping of dishes)
            when 
                find_swap_information.sum_added_dish + find_swap_information.sum_removed_dish != abs(find_swap_information.meals_mealbox - find_swap_information.meals_mealbox_subscription)
            then true
            else false
        end as has_swap
    from add_product_columns
    left join find_swap_information
        on add_product_columns.billing_agreement_order_id = find_swap_information.billing_agreement_order_id

)

, add_recipe_information as (

    select
        add_swap_information.*
        , recipe_feedback.recipe_rating_id
        , recipe_feedback.recipe_comment_id
        , recipe_feedback.recipe_rating
        , recipe_feedback.recipe_rating_score
        , recipe_feedback.is_not_cooked_dish
        , recipe_feedback.recipe_comment
        , greatest(
            add_swap_information.source_created_at
            , recipe_feedback.source_updated_at
        ) as fact_updated_at
        , recipe_costs_and_co2.total_ingredient_weight
        , recipe_costs_and_co2.total_ingredient_weight_whole_units
        , recipe_costs_and_co2.total_ingredient_planned_cost
        , recipe_costs_and_co2.total_ingredient_planned_cost_whole_units
        , recipe_costs_and_co2.total_ingredient_expected_cost
        , recipe_costs_and_co2.total_ingredient_expected_cost_whole_units
        , recipe_costs_and_co2.total_ingredient_actual_cost
        , recipe_costs_and_co2.total_ingredient_actual_cost_whole_units
        , recipe_costs_and_co2.total_ingredient_co2_emissions
        , recipe_costs_and_co2.total_ingredient_co2_emissions_whole_units
        , recipe_costs_and_co2.total_ingredient_weight_with_co2_data
        , recipe_costs_and_co2.total_ingredient_weight_with_co2_data_whole_units

    from add_swap_information
    left join recipe_feedback
        on add_swap_information.recipe_id = recipe_feedback.recipe_id
        and add_swap_information.billing_agreement_id = recipe_feedback.billing_agreement_id
    left join recipe_costs_and_co2
        on add_swap_information.company_id = recipe_costs_and_co2.company_id
        and add_swap_information.menu_year = recipe_costs_and_co2.menu_year
        and add_swap_information.menu_week = recipe_costs_and_co2.menu_week
        and add_swap_information.recipe_id = recipe_costs_and_co2.recipe_id
        and add_swap_information.recipe_portion_id = recipe_costs_and_co2.recipe_portion_id

)

-- Key generation
, add_keys as (

    select 
        md5(concat_ws('-'
            , add_recipe_information.billing_agreement_order_id
            , add_recipe_information.billing_agreement_order_line_id
            , add_recipe_information.product_variation_id
            , add_recipe_information.product_variation_id_subscription
            , add_recipe_information.recipe_id
            , add_recipe_information.recipe_id_subscription
            )
        ) as pk_fact_orders
        , add_recipe_information.* 
        , billing_agreements_ordergen.pk_dim_billing_agreements as fk_dim_billing_agreements_ordergen
        , coalesce(billing_agreements_subscription.pk_dim_billing_agreements, billing_agreements_ordergen.pk_dim_billing_agreements) as fk_dim_billing_agreements_subscription
        , coalesce(billing_agreements_subscription.preference_combination_id, billing_agreements_ordergen.preference_combination_id) as fk_dim_preference_combinations
        --, md5(add_recipe_information.billing_agreement_basket_deviation_origin_id) as fk_dim_basket_deviation_origins
        --, md5(add_recipe_information.subscription_billing_agreement_basket_deviation_origin_id) as fk_dim_basket_deviation_origins_preselected
        , md5(add_recipe_information.company_id) as fk_dim_companies
        , md5(cast( customer_journey_segments.sub_segment_id as string)) as fk_dim_customer_journey_segments
        , cast(date_format(add_recipe_information.menu_week_financial_date, 'yyyyMMdd') as int) as fk_dim_date
        , coalesce(md5(discounts.discount_id), '0') as fk_dim_discounts
        , md5(concat(loyalty_seasons.company_id,loyalty_seasons.loyalty_season_start_date)) as fk_dim_loyalty_seasons
        , coalesce(meals, 0) as fk_dim_meals
        , coalesce(meals_subscription, 0) as fk_dim_meals_subscription
        , coalesce(meals_mealbox, 0) as fk_dim_meals_mealbox
        , coalesce(meals_mealbox_subscription, 0) as fk_dim_meals_mealbox_subscription
        , md5(add_recipe_information.order_status_id) as fk_dim_order_statuses
        , md5(add_recipe_information.order_type_id) as fk_dim_order_types
        , coalesce(md5(concat(add_recipe_information.order_line_type_name, order_line_details)), '0') as fk_dim_order_line_details
        , datediff(add_recipe_information.menu_week_monday_date, billing_agreements_ordergen.first_menu_week_monday_date) as fk_dim_periods_since_first_menu_week
        , coalesce(md5(concat(add_recipe_information.portion_id, add_recipe_information.language_id)), '0') as fk_dim_portions
        , coalesce(md5(concat(add_recipe_information.portion_id_subscription, add_recipe_information.language_id)), '0') as fk_dim_portions_subscription
        , coalesce(md5(concat(add_recipe_information.portion_id_mealbox, add_recipe_information.language_id)), '0') as fk_dim_portions_mealbox
        , coalesce(md5(concat(add_recipe_information.portion_id_mealbox_subscription, add_recipe_information.language_id)), '0') as fk_dim_portions_mealbox_subscription
        , coalesce(
            md5(
                concat(
                    add_recipe_information.product_variation_id,
                    add_recipe_information.company_id
                    )
                ), '0'
            ) as fk_dim_products
        , coalesce(
            md5(
                concat(
                    add_recipe_information.product_variation_id_subscription,
                    add_recipe_information.company_id
                    )
                ), '0'
            ) as fk_dim_products_subscription
        , coalesce(
            md5(
                cast(
                    concat(
                        add_recipe_information.recipe_id,
                        add_recipe_information.language_id
                        ) as string
                    )
                ), '0'
            ) as fk_dim_recipes
        , coalesce(
            md5(
                cast(
                    concat(
                        add_recipe_information.recipe_id_subscription,
                        add_recipe_information.language_id
                        ) as string
                    )
                ), '0'
            ) as fk_dim_recipes_subscription
        
    from add_recipe_information
    left join billing_agreements as billing_agreements_ordergen
        on add_recipe_information.billing_agreement_id = billing_agreements_ordergen.billing_agreement_id
        and add_recipe_information.source_created_at >= billing_agreements_ordergen.valid_from
        and add_recipe_information.source_created_at < billing_agreements_ordergen.valid_to
    -- TODO: temp solution for finding billing_agreements_subscription
    left join subscription_orders
        on add_recipe_information.billing_agreement_order_id = subscription_orders.billing_agreement_order_id
        and subscription_orders.basket_type_id = '{{ var("mealbox_basket_type_id") }}' 
    left join billing_agreements as billing_agreements_subscription
        on add_recipe_information.billing_agreement_id = billing_agreements_subscription.billing_agreement_id
        and subscription_orders.order_placed_at >= billing_agreements_subscription.valid_from
        and subscription_orders.order_placed_at < billing_agreements_subscription.valid_to
    left join discounts
        on add_recipe_information.billing_agreement_order_line_id = discounts.billing_agreement_order_line_id
        and add_recipe_information.menu_year > 2020
    left join loyalty_seasons
        on add_recipe_information.company_id = loyalty_seasons.company_id
        and add_recipe_information.source_created_at >= loyalty_seasons.loyalty_season_start_date
        and add_recipe_information.source_created_at < loyalty_seasons.loyalty_season_end_date
    left join customer_journey_segments
        on add_recipe_information.billing_agreement_id = customer_journey_segments.billing_agreement_id
        and add_recipe_information.menu_week_financial_date >= customer_journey_segments.menu_week_monday_date_from
        and add_recipe_information.menu_week_financial_date < customer_journey_segments.menu_week_monday_date_to
    -- TODO: remove all order lines that are subscribed groceries which was not ordered, and mealboxes that does not match the ordered mealbox
    --where not (is_subscribed_grocery = true and is_grocery = false)
    --and not (is_subscribed_mealbox = true and is_mealbox = false)

)

select * from add_keys
