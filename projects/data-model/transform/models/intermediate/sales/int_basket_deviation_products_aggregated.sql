with

deviation_products as (

    select * from {{ ref('int_basket_deviation_products_joined') }}
    
)


, deviations_aggregated as (

    select
        menu_week_monday_date
        , menu_year
        , menu_week
        , billing_agreement_id
        , billing_agreement_basket_id
        , max(
            case
                when billing_agreement_basket_deviation_origin_id in (
                    '{{ var("preselector_origin_id") }}'
                    , '{{ var("mealselector_origin_id") }}'
                )
                then true
                else false
                end
        ) as has_recommendation
        , max(is_onesub_migration_insert + is_onesub_migration_update) as is_onesub_migration
        , max(
            case
                when is_order_placement
                then deviation_created_at
                end
        ) as order_placed_at
    from deviation_products
    group by all
    -- remove baskets that only has inactive deviations
    having max(is_active_deviation) is true

)


select * from deviations_aggregated
