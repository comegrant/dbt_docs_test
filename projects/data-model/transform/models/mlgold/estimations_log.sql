with

estimations as (

    select * from {{ ref('int_estimations') }}

)

, estimations_log_history as (

    select * from {{ ref('analyticsdb_cms__estimations_log') }}

)

, dim_products as (

    select * from {{ ref('dim_products') }}

)

, estimations_grouped as (

    select

        menu_year
        , menu_week
        , company_id
        , product_variation_id
        , billing_agreement_basket_deviation_origin_id
        , estimation_generated_at
        , sum(product_variation_quantity) as product_variation_quantity

    from estimations
    group by 1, 2, 3, 4, 5, 6

)

, agreements_with_mealbox_adjustments as (

    select

        menu_year
        , menu_week
        , estimations.company_id
        , '10000000-0000-0000-0000-000000000000'           as product_variation_id
        , billing_agreement_basket_deviation_origin_id
        , estimation_generated_at
        , count(distinct estimations.billing_agreement_id) as product_variation_quantity

    from estimations
    left join dim_products
        on
            estimations.product_variation_id = dim_products.product_variation_id
            and estimations.company_id = dim_products.company_id
    where
        --Non-deviations
        estimations.billing_agreement_basket_deviation_origin_id <> '00000000-0000-0000-0000-000000000000'
        and dim_products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
        and estimations.company_id in ({{ var('active_company_ids') | join(', ') }})

    group by 1, 2, 3, 4, 5, 6

)

, estimations_and_adjustments_unioned as (

    select * from estimations_grouped

    union all

    select * from agreements_with_mealbox_adjustments

)

, add_pk as (

    select
        md5(
            concat(
                cast(menu_year as string)
                , cast(menu_week as string)
                , cast(company_id as string)
                , cast(product_variation_id as string)
                , cast(billing_agreement_basket_deviation_origin_id as string)
                , cast(estimation_generated_at as string)
            )
        )       as pk_estimations_log
        , menu_year
        , menu_week
        , company_id
        , product_variation_id
        , billing_agreement_basket_deviation_origin_id
        , product_variation_quantity
        , estimation_generated_at
        , 'ndp' as source

    from estimations_and_adjustments_unioned
)


, estimations_log_history_filtered as (

    select

        estimations_log_id as pk_estimations_log
        , menu_year
        , menu_week
        , company_id
        , product_variation_id
        , billing_agreement_basket_deviation_origin_id
        , product_variation_quantity
        , estimation_generated_at
        , 'adb_history'    as source

    from estimations_log_history
    where
        estimation_generated_at
        < (
            select min(estimation_generated_at) as min_generated_at
            from estimations
        )

)

, estimations_and_history_unioned as (

    select * from add_pk

    union all

    select * from estimations_log_history_filtered

)


select * from estimations_and_history_unioned
