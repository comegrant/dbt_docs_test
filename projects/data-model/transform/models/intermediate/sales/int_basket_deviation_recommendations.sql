with

deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}

)

, deviation_recommendations_recency_group as (

    select
        deviations.*
        , dense_rank()
            over
            (

                partition by
                    deviations.menu_week_monday_date
                    , deviations.billing_agreement_basket_id
                order by
                    deviations.deviation_created_at desc

            )
        as recommendation_recency_group
    from deviations
    where billing_agreement_basket_deviation_origin_id in (
        '{{ var("mealselector_origin_id") }}'
        , '{{ var("preselector_origin_id") }}'
    )

)

select * from deviation_recommendations_recency_group
