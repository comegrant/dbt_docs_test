with

discounts as (

    select * from {{ ref('cms__discounts') }}

)

, discount_channels as (

    select * from {{ ref('cms__discount_channels') }}

)

, discount_categories as (

    select * from {{ ref('cms__discount_categories') }}

)

, discount_sub_categories as (

    select * from {{ ref('cms__discount_sub_categories') }}

)

, discount_chains as (

    select * from {{ ref('int_discount_chains_unioned') }}

)

, discount_tables_joined as (

    select
    md5(discounts.discount_id) as pk_dim_discounts
        , discounts.discount_id
        , discounts.discount_usage_type_id
        , discounts.discount_type_id
        , discounts.discount_amount_type_id
        , discounts.discount_category_id
        , discounts.discount_channel_id
        , discounts.discount_sub_category_id
        , discounts.discount_coupon_type_id
        , discounts.discount_title
        , discounts.discount_amount
        , discount_channels.discount_channel_name
        , discount_categories.discount_category_name
        , discount_sub_categories.discount_sub_category_name
        , discounts.discount_partner_invoice_reference
        , discounts.customer_usage_limit
        , discounts.discount_partner_price
        , discounts.is_active_discount
        , discounts.is_coupon_code_required
        , discounts.is_cumulative_discount
        , discounts.is_valid_on_direct_order
        , discounts.is_registration_discount
        , discounts.is_disabled_on_freeze
        , discounts.is_highest_price_discount
        , discount_chains.discount_parent_id
        , discount_chains.discount_chain_order
        , case when discount_chains.discount_parent_id is not null
            then true
            else false
            end as is_discount_chain

    from discounts
    left join discount_channels on discounts.discount_channel_id = discount_channels.discount_channel_id
    left join discount_categories on discounts.discount_category_id = discount_categories.discount_category_id
    left join discount_sub_categories on discounts.discount_sub_category_id = discount_sub_categories.discount_sub_category_id
    left join discount_chains on discounts.discount_id = discount_chains.discount_id

)

, add_unknown_row as (

    select *
    from discount_tables_joined

    union all

    select
        '0' as pk_dim_discounts
        , 0 as discount_id
        , 0 as discount_usage_type_id
        , 0 as discount_type_id
        , 0 as discount_amount_type_id
        , 0 as discount_category_id
        , 0 as discount_channel_id
        , 0 as discount_sub_category_id
        , 0 as discount_coupon_type_id
        , 'Not relevant'as discount_title
        , 0 as discount_amount
        , 'Not relevant'as discount_channel_name
        , 'Not relevant'as discount_category_name
        , 'Not relevant'as discount_sub_category_name
        , 0 as discount_partner_invoice_reference
        , 0 as customer_usage_limit
        , 0 as discount_partner_price
        , false as is_active_discount
        , false as is_coupon_code_required
        , false as is_cumulative_discount
        , false as is_valid_on_direct_order
        , false as is_registration_discount
        , false as is_disabled_on_freeze
        , false as is_highest_price_discount
        , 0 as discount_parent_id
        , 0 as discount_chain_order
        , false as is_discount_chain

)

select * from add_unknown_row