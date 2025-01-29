with 

dim_billing_agreements as (

    select * from {{ ref('dim_billing_agreements') }}

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
    , row_number() over (
        partition by billing_agreement_id 
        order by 
            case
                when 
                    billing_agreement_status_name is not null
                    and billing_agreement_basket_product_updated_id is not null
                    and billing_agreement_preferences_updated_id is not null
                    and loyalty_level_number is not null
                    and onesub_flag is not null
                then 1 --Non null values should be sorted before null values
                else 2 --Null values should be sorted after null values
            end asc,    
            valid_from asc
    ) as rank

    from dim_billing_agreements
    
    -- TODO: Remove this filter when we have handled these cases in dim_billing_agreements. See DATA-423.
    where valid_from <> valid_to
    
    -- TODO: Modify this filter when we know how far back we trust the history of this table
    and valid_from >= '2015-01-01' 
)

, updated_and_previous_version_joined as (

    select 
    agreements.fk_dim_billing_agreements as pk_fact_billing_agreement_updates
    , cast(date_format(agreements.valid_from, 'yyyyMMdd') as int) as fk_dim_dates
    , cast(date_format(agreements.valid_from, 'HHmm') as string) as fk_dim_time
    , agreements.fk_dim_billing_agreements
    , agreements_previous_version.fk_dim_billing_agreements as fk_dim_billing_agreements_previous_version
    , md5(agreements.company_id) as fk_dim_companies

    , case
        when agreements.valid_from < agreements.first_menu_week_monday_date then 0
        else datediff(agreements.valid_from, agreements.first_menu_week_monday_date)
    end as fk_dim_periods_since_first_order

    , agreements.billing_agreement_id
    , agreements.valid_from as updated_at
    
    , case
        when agreements.rank = 1 then true
        else false
    end as is_new_agreement

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
        when agreements_previous_version.fk_dim_billing_agreements is null then false
        when agreements.onesub_flag <> agreements_previous_version.onesub_flag then true
        else false
    end as has_updated_onesub_flag

    from agreements
    left join agreements as agreements_previous_version
        on agreements.billing_agreement_id = agreements_previous_version.billing_agreement_id
        and agreements.valid_from = agreements_previous_version.valid_to

)

select * from  updated_and_previous_version_joined