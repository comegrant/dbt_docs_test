with

source as (

    select * from {{ source('cms', 'cms__discount') }}

)

, renamed as (

    select

    {# ids #}
        -- place ids here
        discount_id
        , discount_usage_type_id
        , discount_type_id
        , discount_amount_type_id
        , company_id
        , category_id               as discount_category_id
        , discount_channel_id
        , discount_sub_category_id
        , coupon_code_type          as discount_coupon_type_id

        {# strings #}
        -- place strings here
        , title_                    as discount_title
        , partner_invoice_reference as discount_partner_invoice_reference

        {# numerics #}
        -- place numerics here
        , initial_usage             as customer_usage_limit
        , discount_amount
        , partner_price             as discount_partner_price

        {# booleans #}
        -- place booleans here
        , is_active                 as is_active_discount
        , is_coupon_code_required
        , is_cumulative             as is_cumulative_discount
        , is_direct_order           as is_valid_on_direct_order
        , is_registration_only      as is_registration_discount
        , disable_on_freeze         as is_disabled_on_freeze
        , highest_price             as is_highest_price_discount

        {# date #}
        -- place dates here
        , valid_start_date          as discount_valid_from
        , valid_end_date            as discount_valid_to
        , registration_start_date   as discount_registration_valid_from
        , registration_end_date     as discount_registration_valid_to

        {# timestamp #}
        -- place timestamps here

        {# scd #}
        -- place slowly change dimension fields here

        {# system #}
        -- place system columns here
        , created_date              as source_created_at
        , modified_date             as source_updated_at
        , modified_user_name        as source_updated_by
        , created_user_name         as source_created_by

    from source

)

select * from renamed
