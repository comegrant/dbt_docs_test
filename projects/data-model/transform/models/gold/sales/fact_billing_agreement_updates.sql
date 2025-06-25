with

dim_billing_agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, billing_agreement_preferences as (

    select * from {{ ref('int_billing_agreement_preferences_unioned') }}

)

, agreements as (

    select
    billing_agreement_id
    , valid_from
    , valid_to
    , pk_dim_billing_agreements as fk_dim_billing_agreements
    , company_id
    , first_menu_week_monday_date
    , billing_agreement_preferences_updated_id
    , billing_agreement_basket_product_updated_id
    , billing_agreement_status_name
    , loyalty_level_number
    , onesub_flag

    -- Rank the rows based on criteria in order to select the version of the new agreement that has the most complete data
    , row_number() over (
        partition by billing_agreement_id
        order by
            case
                when
                    signup_date = date(valid_from)
                    and billing_agreement_status_name is not null
                    and billing_agreement_basket_product_updated_id is not null
                    and billing_agreement_preferences_updated_id is not null
                    and loyalty_level_number is not null
                    and onesub_flag is not null
                then 1 --Non null values should be sorted before null values
                else 2 --Null values should be sorted after null values
            end asc,
            valid_from asc
    ) as signup_rank

    from dim_billing_agreements

    -- TODO: Remove this filter when we have handled these cases in dim_billing_agreements. See DATA-423.
    where valid_from <> valid_to

    -- TODO: Modify this filter when we know how far back we trust the history of this table
    and valid_from >= '2015-01-01'
)

, loyalty_seasons as (

    select * from {{ ref('dim_loyalty_seasons') }}

)

, updated_and_previous_version_joined as (

    select
    agreements.fk_dim_billing_agreements as pk_fact_billing_agreement_updates
    , cast(date_format(agreements.valid_from, 'yyyyMMdd') as int) as fk_dim_dates
    , cast(date_format(agreements.first_menu_week_monday_date, 'yyyyMMdd') as int) as fk_dim_dates_first_menu_week
    , cast(date_format(agreements.valid_from, 'HHmm') as string) as fk_dim_time
    , agreements.fk_dim_billing_agreements
    , agreements_previous_version.fk_dim_billing_agreements as fk_dim_billing_agreements_previous_version
    , billing_agreement_preferences.preference_combination_id as fk_dim_preference_combinations
    , billing_agreement_preferences_previous_version.preference_combination_id as fk_dim_preference_combinations_previous_version
    , md5(agreements.company_id) as fk_dim_companies
    , loyalty_seasons.pk_dim_loyalty_seasons as fk_dim_loyalty_seasons

    , case
        when agreements.valid_from < agreements.first_menu_week_monday_date then 0
        else datediff(agreements.valid_from, agreements.first_menu_week_monday_date)
    end as fk_dim_periods_since_first_order

    , agreements.billing_agreement_id
    , agreements.valid_from as updated_at
    , agreements.first_menu_week_monday_date

    , case
        when agreements.signup_rank = 1 then true
        else false
    end as is_signup

    , case
        when agreements.first_menu_week_monday_date >= agreements.valid_from and agreements.first_menu_week_monday_date < agreements.valid_to then true
        else false
    end as has_first_delivery

    , case
        when agreements_previous_version.fk_dim_billing_agreements is null then false
        when agreements.billing_agreement_preferences_updated_id <> agreements_previous_version.billing_agreement_preferences_updated_id then true
        else false
    end as has_updated_preferences

    , case
        when agreements_previous_version.fk_dim_billing_agreements is null then false
        when agreements.billing_agreement_basket_product_updated_id <> agreements_previous_version.billing_agreement_basket_product_updated_id then true
        else false
    end as has_updated_subscribed_products

    , case
        when agreements_previous_version.fk_dim_billing_agreements is null then false
        when agreements.billing_agreement_status_name <> agreements_previous_version.billing_agreement_status_name then true
        else false
    end as has_updated_status

    , case
        when agreements_previous_version.fk_dim_billing_agreements is null then false
        when agreements.loyalty_level_number <> agreements_previous_version.loyalty_level_number then true
        else false
    end as has_updated_loyalty_level

    , case 
        when agreements_previous_version.loyalty_level_number < agreements.loyalty_level_number then true
        else false
    end as has_upgraded_loyalty_level

    , case 
        when agreements_previous_version.loyalty_level_number > agreements.loyalty_level_number then true
        else false
    end as has_downgraded_loyalty_level

    , case
        when agreements_previous_version.fk_dim_billing_agreements is null then false
        when agreements.onesub_flag <> agreements_previous_version.onesub_flag then true
        else false
    end as has_updated_onesub_flag

    from agreements
    left join agreements as agreements_previous_version
        on agreements.billing_agreement_id = agreements_previous_version.billing_agreement_id
        and agreements.valid_from = agreements_previous_version.valid_to
    left join billing_agreement_preferences
        on agreements.billing_agreement_preferences_updated_id = billing_agreement_preferences.billing_agreement_preferences_updated_id
    left join billing_agreement_preferences as billing_agreement_preferences_previous_version
        on agreements_previous_version.billing_agreement_preferences_updated_id = billing_agreement_preferences_previous_version.billing_agreement_preferences_updated_id
    left join loyalty_seasons 
        on agreements.company_id = loyalty_seasons.company_id
        and agreements.valid_from >= loyalty_seasons.loyalty_season_start_date
        and agreements.valid_from < loyalty_seasons.loyalty_season_end_date

)

select * from updated_and_previous_version_joined

