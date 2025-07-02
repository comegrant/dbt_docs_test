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

, order_line_details as (
        
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
        
        select 'Campaign Discount'


)

, order_line_types_and_details_joined as (

    select 
        order_line_type_name
        , order_line_details
    from order_line_types
    full join order_line_details

    union all
    
    select
        order_line_type_name  as order_line_type_name
        , order_line_type_name  as order_line_details 
    from order_line_types

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