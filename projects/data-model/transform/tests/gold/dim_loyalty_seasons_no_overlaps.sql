with order_loyalty_seasons_per_company_and_add_next_season_start_date as (
    select
        company_id
        , loyalty_season_start_date
        , loyalty_season_end_date
        , lead(loyalty_season_start_date) over (partition by company_id order by loyalty_season_start_date) as next_season_start_date
    from {{ ref('dim_loyalty_seasons') }}
)

select *
from order_loyalty_seasons_per_company_and_add_next_season_start_date
where loyalty_season_end_date > next_season_start_date
