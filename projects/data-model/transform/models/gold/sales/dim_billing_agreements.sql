with 

billing_agreements as (

    select * from {{ ref('cms__billing_agreements') }}

)

, first_orders as (

    select * 
    from {{ ref('int_billing_agreements_extract_first_order') }}

)

, status_names as (

    select * from {{ ref('cms__billing_agreement_statuses') }}

)

, preferences_scd as (

    select * from {{ref('int_billing_agreement_preferences_unioned')}}

)

/*, scd_loyalty_level as (

    select * from {{ ref('int_billing_agreements_loyalty_levels_scd') }}

) */

, current as (
    select * from billing_agreements
    where valid_to is null
)

, billing_agreements_base as (
    select 
    current.billing_agreement_id
    , case 
        when first_orders.source_created_at < current.signup_at
        then first_orders.source_created_at
        else current.signup_at 
        end as valid_from
    , {{ get_scd_valid_to() }} as valid_to

    from current
    left join first_orders
    on current.billing_agreement_id = first_orders.billing_agreement_id

)

, billing_agreements_scd2 as (

    select
        md5(
            concat(
                cast(billing_agreements.billing_agreement_id as string)
                , cast(billing_agreements.valid_from as string)
            )
        ) as billing_agreements_scd2_id
        , billing_agreements.billing_agreement_id
        , status_names.billing_agreement_status_name
        , billing_agreements.sales_point_id
        , billing_agreements.valid_from
        , {{ get_scd_valid_to('billing_agreements.valid_to') }} as valid_to
    from billing_agreements
    left join status_names
    on billing_agreements.billing_agreement_status_id = status_names.billing_agreement_status_id
)

, unified_timeline_part1 as (

    {{join_snapshots('billing_agreements_base', 'billing_agreements_scd2', 'billing_agreement_id', 'billing_agreements_scd2_id')}}

)

, unified_timeline as (

    {{join_snapshots('unified_timeline_part1', 'preferences_scd', 'billing_agreement_id', 'billing_agreement_preferences_updated_id')}}

)

{# TODO join in loyalty as well #}

, all_tables_joined as (

    select
    md5(concat(
        cast(unified_timeline.billing_agreement_id as string),
        cast(unified_timeline.valid_from as string)
        )
    ) as pk_dim_billing_agreements
    , cast(unified_timeline.billing_agreement_id as int) as billing_agreement_id
    , current.company_id
    , billing_agreements_scd2.sales_point_id
    , unified_timeline.billing_agreement_preferences_updated_id 
    , unified_timeline.valid_from
    , unified_timeline.valid_to
    , case 
        when unified_timeline.valid_to = '9999-01-01'
        then true 
        else false 
    end as is_current
    , current.payment_method
    , current.signup_source
    , current.signup_salesperson
    , billing_agreements_scd2.billing_agreement_status_name
    , current.signup_date
    , current.signup_year_day
    , current.signup_month_day
    , current.signup_week_day
    , current.signup_week
    , current.signup_month
    , current.signup_quarter
    , current.signup_year
    , first_orders.first_menu_week_monday_date
    , first_orders.first_menu_week_week
    , first_orders.first_menu_week_month
    , first_orders.first_menu_week_quarter
    , first_orders.first_menu_week_year
    from unified_timeline
    left join current
    on unified_timeline.billing_agreement_id = current.billing_agreement_id
    left join first_orders
    on unified_timeline.billing_agreement_id = first_orders.billing_agreement_id
    left join billing_agreements_scd2
    on unified_timeline.billing_agreements_scd2_id = billing_agreements_scd2.billing_agreements_scd2_id
)

select * from all_tables_joined