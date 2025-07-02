with 

baskets as (

    select * from {{ ref('cms__billing_agreement_baskets') }}
    where valid_to = '{{ var("future_proof_date") }}'

)

, billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}
    where valid_to = '{{ var("future_proof_date") }}'

)

, deviations as (

    select * from {{ ref('cms__billing_agreement_basket_deviations') }}

)

, deviation_products as (

    select * from {{ ref('cms__billing_agreement_basket_deviation_products') }}

)

, deviation_version as (

    select
        deviations.billing_agreement_basket_deviation_id
        , dense_rank() over (
            partition by
                deviations.billing_agreement_basket_id
                , deviations.menu_week_monday_date
            order by
                deviations.source_created_at asc
        ) as deviation_version
    from deviations

)

, recommendation_version as (

    select
        deviations.billing_agreement_basket_deviation_id
        , dense_rank()
            over
            (

                partition by
                    deviations.billing_agreement_basket_id
                    , deviations.menu_week_monday_date
                order by
                    deviations.source_created_at desc

            )
        as recommendation_version_desc
    from deviations
    where billing_agreement_basket_deviation_origin_id in (
        '{{ var("mealselector_origin_id") }}'
        , '{{ var("preselector_origin_id") }}'
    )

)

-- find the deviation of the first placed order
-- i.e. after this changes to the customers subscription 
-- will only be valid for future menu weeks
, order_placement as (

    select
        billing_agreement_basket_id
        , menu_week_monday_date
        , coalesce(

            -- get the id of the latest deviation created by the preselector or mealselector
            max_by(
                billing_agreement_basket_deviation_id
                , case 
                    when billing_agreement_basket_deviation_origin_id in 
                    (
                        '{{ var("preselector_origin_id") }}'
                        ,'{{ var("mealselector_origin_id") }}'
                    )
                    then source_created_at
                    else null
                    end
            )
            
            -- get the id of the first deviation created by the customer
            , min_by(
                billing_agreement_basket_deviation_id
                , case 
                    when billing_agreement_basket_deviation_origin_id = '{{ var("normal_origin_id") }}'
                    then source_created_at
                    else null
                    end
            )
        
        ) as billing_agreement_basket_deviation_id
    from deviations
    group by all

)

, basket_deviation_tables_joined as (

    select
        baskets.billing_agreement_id
        , baskets.billing_agreement_basket_id
        , baskets.basket_type_id
        , billing_agreements.company_id
        , deviations.billing_agreement_basket_deviation_id
        , deviations.billing_agreement_basket_deviation_origin_id
        , deviation_products.billing_agreement_basket_deviation_product_id
        , deviation_products.product_variation_id
        , deviations.delivery_week_type_id
        , deviations.menu_week_monday_date
        , deviations.menu_year
        , deviations.menu_week
        , deviation_products.product_variation_quantity
        , deviation_version.deviation_version
        , recommendation_version.recommendation_version_desc
        , deviations.is_active_deviation
        , deviation_products.is_extra_product
        -- new deviation was created to migrate customer to onesub
        , deviations.is_onesub_migration as is_onesub_migration_insert
        -- previous deviation was updated to migrated customer to onesub
        , deviation_products.is_onesub_migration as is_onesub_migration_update
        , (order_placement.billing_agreement_basket_deviation_id is not null) as is_order_placement
        , deviations.source_created_at as deviation_created_at
        , deviations.source_created_by as deviation_created_by
        , deviations.source_updated_at as deviation_updated_at
        , deviations.source_updated_by as deviation_updated_by
        , deviation_products.source_created_at as deviation_product_created_at
        , deviation_products.source_created_by as deviation_product_created_by
        , deviation_products.source_updated_at as deviation_product_updated_at
        , deviation_products.source_updated_by as deviation_product_updated_by
    from deviations
    left join deviation_products
      on deviations.billing_agreement_basket_deviation_id = deviation_products.billing_agreement_basket_deviation_id
    left join baskets
      on deviations.billing_agreement_basket_id = baskets.billing_agreement_basket_id
    left join billing_agreements
        on billing_agreements.billing_agreement_id = baskets.billing_agreement_id
    left join deviation_version
        on deviations.billing_agreement_basket_deviation_id = deviation_version.billing_agreement_basket_deviation_id
    left join recommendation_version
        on deviations.billing_agreement_basket_deviation_id = recommendation_version.billing_agreement_basket_deviation_id
    left join order_placement
        on deviations.billing_agreement_basket_deviation_id = order_placement.billing_agreement_basket_deviation_id

)

select * from basket_deviation_tables_joined