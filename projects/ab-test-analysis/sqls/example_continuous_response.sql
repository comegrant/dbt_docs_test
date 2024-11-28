with mealbox_orders as (
    select
        billing_agreement_id,
        menu_year,
        menu_week,
        total_amount_inc_vat
    from gold.fact_orders
    where (menu_year * 100 + menu_week) > 202440
    and is_dish = false
),

loyalty_points as (
    select
        billing_agreement_id,
        row_number() over (partition by billing_agreement_id order by accrued_points desc) as rank,
        accrued_points,
        loyalty_level_id
    from silver.cms__loyalty_agreement_ledger
),

max_loyalty as (
    select
        billing_agreement_id,
        accrued_points,
        loyalty_level_id
    from loyalty_points
    where rank = 1
),

amount_spent as (
    select
        billing_agreement_id,
        sum(total_amount_inc_vat) as amount_spent
    from mealbox_orders
    group by
        billing_agreement_id
)

select
    amount_spent.*,
    accrued_points,
    case
        when rand() < 0.5 then 'a'
        else 'b'
    end as random_group,
    case
        when accrued_points > 5000 then 'a'
        else 'b'
    end as non_random_group
from
    amount_spent
left join
    max_loyalty
where amount_spent.billing_agreement_id = max_loyalty.billing_agreement_id
