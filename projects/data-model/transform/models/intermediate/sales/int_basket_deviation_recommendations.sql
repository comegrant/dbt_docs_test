with

deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}

)

, first_customer_deviation as (

    select
        menu_week_monday_date
        , billing_agreement_id
        , billing_agreement_basket_id
        , min(deviation_created_at) as first_deviation_created_at
    from deviations
    where
        billing_agreement_basket_deviation_origin_id not in (
            '{{ var("mealselector_origin_id") }}'
            , '{{ var("preselector_origin_id") }}'
        )
    group by 1, 2, 3

)

, deviation_recommendations_filter as (

    select deviations.*
    from deviations
    left join first_customer_deviation
        on
            deviations.menu_week_monday_date = first_customer_deviation.menu_week_monday_date
            and deviations.billing_agreement_basket_id = first_customer_deviation.billing_agreement_basket_id
            and deviations.billing_agreement_id = first_customer_deviation.billing_agreement_id
    where billing_agreement_basket_deviation_origin_id in (
        '{{ var("mealselector_origin_id") }}'
        , '{{ var("preselector_origin_id") }}'
    )
    and (
        deviations.deviation_created_at < first_customer_deviation.first_deviation_created_at
        or first_customer_deviation.first_deviation_created_at is null
    )

)

, deviation_recommendations_grouping as (

    select
        deviation_recommendations_filter.*
        , dense_rank()
            over
            (

                partition by
                    deviation_recommendations_filter.menu_week_monday_date
                    , deviation_recommendations_filter.billing_agreement_basket_id
                order by
                    deviation_recommendations_filter.deviation_created_at desc

            )
        as recommendation_recency_group
    from deviation_recommendations_filter

)

select * from deviation_recommendations_grouping
