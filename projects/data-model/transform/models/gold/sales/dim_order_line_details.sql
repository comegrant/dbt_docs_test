with

order_lines as (

    select * from {{ ref('int_billing_agreement_order_lines_joined') }}

)

, products as (

    select * from {{ ref('int_product_tables_joined') }}

)

, create_order_line_details as (
    select distinct
        order_lines.order_line_type_name
        , case
            when
                products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
                and order_lines.total_amount_ex_vat > 0
                then 'Plus Price Dish'
            when
                products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
                and order_lines.total_amount_ex_vat < 0
                then 'Thrifty Dish'
            when
                products.product_type_id = '{{ var("velg&vrak_product_type_id") }}'
                and order_lines.total_amount_ex_vat = 0
                then 'Normal Dish'
            when
                products.product_type_id in (
                    '{{ var("mealbox_product_type_id") }}'
                    , '{{ var("financial_product_type_id") }}'
                )
                then 'Mealbox'
            when products.product_type_id in ({{ var('grocery_product_type_ids') | join(', ') }})
                then 'Groceries'
            else order_lines.order_line_type_name
        end as order_line_details
    from order_lines
    left join products on order_lines.product_variation_id = products.product_variation_id

)

, add_pk as (

    select
        md5(
            concat(
                create_order_line_details.order_line_type_name, create_order_line_details.order_line_details
            )
        ) as pk_dim_order_line_details
        , create_order_line_details.*
    from create_order_line_details

    union all

    select
        '0'              as pk_dim_order_line_details
        , 'Not relevant' as order_line_type_name
        , 'Not relevant' as order_line_details

)

select * from add_pk
