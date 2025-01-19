with deviations as (

    select * from {{ ref('int_basket_deviation_products_joined') }}

)

-- TODO: add this to the silver table
, add_deviation_version as (

    select
        deviations.*
        , dense_rank() over (
            partition by
                deviations.billing_agreement_id,
                deviations.menu_week,
                deviations.menu_year
            order by
                deviations.deviation_created_at asc
        ) as deviation_version
    from deviations

)

select * from add_deviation_version
