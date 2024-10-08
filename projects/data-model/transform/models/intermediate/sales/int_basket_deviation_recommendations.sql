with 

deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}

)

, deviation_recommendations_filter as (

    select 
       *
    from deviations
    where billing_agreement_basket_deviation_origin_id in (
        '9E016A92-9E5C-4B5B-AC5D-739CEFD6F07B', -- Meal-selector
        '6AD5DF6F-6CDA-4E45-96D0-A169B633247C' -- Pre-selector
        )
)

, deviation_recommendations_grouping as (

    select 
       deviation_recommendations_filter.*,
       dense_rank() over 
       (

            partition by
                menu_week_monday_date
                , billing_agreement_basket_id
            order by 
                source_created_at desc

        ) as recommendation_recency_group
    from deviation_recommendations_filter

)

select * from deviation_recommendations_grouping