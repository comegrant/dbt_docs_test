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

, relevant_columns as (

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
    from discounts
    left join discount_channels on discounts.discount_channel_id = discount_channels.discount_channel_id
    left join discount_categories on discounts.discount_category_id = discount_categories.discount_category_id
    left join discount_sub_categories on discounts.discount_sub_category_id = discount_sub_categories.discount_sub_category_id

)

select * from relevant_columns