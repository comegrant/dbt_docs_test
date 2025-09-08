with 

loyalty_agreement_ledger as (
    
    select * from {{ ref('cms__loyalty_agreement_ledger') }}

)


, loyalty_level_companies as (

    select * from {{ ref('cms__loyalty_level_companies') }}
    where valid_to = '{{ var("future_proof_date") }}'

)


, loyalty_agreement_ledger_with_company as (

    select 
        loyalty_agreement_ledger.loyalty_agreement_ledger_id
        , loyalty_agreement_ledger.billing_agreement_id
        , loyalty_agreement_ledger.loyalty_event_id
        , loyalty_agreement_ledger.loyalty_level_company_id
        , loyalty_agreement_ledger.accrued_points
        , loyalty_agreement_ledger.points_generated_at
        , loyalty_level_companies.company_id
    from loyalty_agreement_ledger
    left join loyalty_level_companies 
        on loyalty_agreement_ledger.loyalty_level_company_id = loyalty_level_companies.loyalty_level_company_id

)


, find_beginning_of_loyalty_seasons as (
-- assumes there will be at least one end of season event per quarter (standard calendar). This should be true
    select 
        company_id
        , year(cast(points_generated_at as date)) as loyalty_season_year
        , quarter(cast(points_generated_at as date)) as loyalty_season_quarter
        , min(cast(points_generated_at as date)) as earliest_event_date
        , min(points_generated_at) as earliest_event_timestamp_at
    from loyalty_agreement_ledger_with_company
    where loyalty_event_id = 'E871C2B7-B3CE-4D23-A502-85343AB7123D' -- end of LP season event
    group by 1,2,3

)


, number_each_loyalty_season_season_per_company as (

    select 
        company_id
        , loyalty_season_year
        , loyalty_season_quarter
        , earliest_event_date
        , earliest_event_timestamp_at
        , row_number() over (partition by company_id order by earliest_event_date) as season_number
    from find_beginning_of_loyalty_seasons

)

-- first ever loyalty period will start with a migration event
-- in Linas, there were multiple days with migration events due to soft launches
, find_first_beginning_of_loyalty_seasons as (

    select 
        company_id
        , min(
            case when company_id = '6A2D0B60-84D6-4830-9945-58D518D27AC2' 
            then '2023-10-03 23:34:16.3900000' 
            else points_generated_at end
        ) as first_loyalty_season_start_date 
    from loyalty_agreement_ledger_with_company
    where loyalty_event_id = '9CB9CAD3-77C5-E711-80C2-0003FF53067B' -- Migration event
    group by 1
)


-- union the first ever loyalty period start date to end of loyalty season dates
, loyalty_season_beginnings_unioned as (

    select 
        company_id
        , year(first_loyalty_season_start_date) as loyalty_season_year
        , quarter(first_loyalty_season_start_date) as loyalty_season_quarter
        , cast(first_loyalty_season_start_date as date) as earliest_event_date
        , first_loyalty_season_start_date as earliest_event_timestamp_at
        , 0 as season_number
    from find_first_beginning_of_loyalty_seasons

    union all 

    select * 
    from number_each_loyalty_season_season_per_company

)

, add_end_of_loyalty_seasons as (

    select 
        t1.company_id
        , concat(t1.loyalty_season_year,' S',t1.loyalty_season_quarter) as loyalty_season_name
        , t1.loyalty_season_year
        , t1.loyalty_season_quarter
        , t1.earliest_event_date as loyalty_season_start_date
        , cast(coalesce(t2.earliest_event_date,'{{ var("future_proof_date") }}') as date) as loyalty_season_end_date
    from loyalty_season_beginnings_unioned t1 
    left join loyalty_season_beginnings_unioned t2 
        on t1.company_id = t2.company_id
        and t1.season_number = t2.season_number-1

)


, add_pk as (

    select 
        md5(concat(company_id,loyalty_season_start_date)) as pk_dim_loyalty_seasons
        , *
    from add_end_of_loyalty_seasons

)

select * from add_pk