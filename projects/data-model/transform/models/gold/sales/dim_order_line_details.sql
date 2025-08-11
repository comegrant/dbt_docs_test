with

order_lines as (

    select * from {{ ref('int_billing_agreement_order_lines_joined') }}

)

, order_line_types as (

    select distinct 
        order_line_type_name
    from order_lines

    union

    select 'GENERATED' as order_line_type_name

)

, order_line_details_debit as (

        select 'Plus Price Dish' as order_line_details
        
        union all
        
        select 'Thrifty Dish'
        
        union all
        
        select 'Normal Dish'
        
        union all
        
        select 'Mealbox'
        
        union all
        
        select 'Groceries'

        union all

        select 'Discount'

        union all

        select 'DEBIT'

)

, order_line_details_generated as (
        
        select 'Normal Dish' as order_line_details
        
        union all
        
        select 'Groceries'

        union all
        
        select 'GENERATED'

)

, order_line_details_discount as (

        select 'Discount' as order_line_details
        
        union all
        
        select 'Campaign Discount'

)

, order_line_types_and_details_joined as (

    select 
        order_line_types.order_line_type_name
        , case
            when order_line_types.order_line_type_name = 'DEBIT'
            then order_line_details_debit.order_line_details
            when order_line_types.order_line_type_name = 'GENERATED'
            then order_line_details_generated.order_line_details
            when order_line_types.order_line_type_name in ('DISCOUNT', 'DISCOUNT_GIFT')
            then order_line_details_discount.order_line_details
            else order_line_types.order_line_type_name
        end as order_line_details
    from order_line_types
    left join order_line_details_debit
        on order_line_types.order_line_type_name = 'DEBIT'
    left join order_line_details_generated
        on order_line_types.order_line_type_name = 'GENERATED'
    left join order_line_details_discount
        on order_line_types.order_line_type_name in ('DISCOUNT', 'DISCOUNT_GIFT')

)

, add_pk_and_uknown_row as (

    select
        md5(
            concat(
                order_line_type_name
                , order_line_details
            )
        ) as pk_dim_order_line_details
        , order_line_types_and_details_joined.*
    from order_line_types_and_details_joined

    union all

    select
        '0'              as pk_dim_order_line_details
        , 'Not relevant' as order_line_type_name
        , 'Not relevant' as order_line_details

)

select * from add_pk_and_uknown_row