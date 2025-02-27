with

channels as (

    select * from {{ ref('cms__discount_channels') }}

)

, categories as (

    select * from {{ ref('cms__discount_categories') }}

)

, sub_categories as (

    select * from {{ ref('cms__discount_sub_categories') }}

)

, channel_category_mapping as (

    select * from {{ ref('cms__discount_mapping_channel_categories') }}

)

, category_sub_category_mapping as (

    select * from {{ ref('cms__discount_category_mapping_discount_sub_categories') }}

)

, category_hierarchy as (

    select
        channels.discount_channel_id
        , channel_category_mapping.discount_category_id
        , category_sub_category_mapping.discount_sub_category_id
        , channels.discount_channel_name
        , categories.discount_category_name
        , sub_categories.discount_sub_category_name
    from channels
    left join channel_category_mapping
        on channels.discount_channel_id = channel_category_mapping.discount_channel_id
    left join category_sub_category_mapping
        on channel_category_mapping.discount_category_id = category_sub_category_mapping.discount_category_id
    left join categories 
        on channel_category_mapping.discount_category_id = categories.discount_category_id
    left join sub_categories
        on category_sub_category_mapping.discount_sub_category_id = sub_categories.discount_sub_category_id

)

select * from category_hierarchy
