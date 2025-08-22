with 

order_types as (

    select * from {{ ref('cms__order_types') }}

)

, mealbox_composition as (

    select
        null as mealbox_composition_id
        , 'Subscription' as order_type_name
        , null as mealbox_selection
        , null as premium_dish
        , null as thrifty_dish

    union all

    select
        1 as mealbox_composition_id
        , 'Subscription' as order_type_name
        , 'Preselected Menu' as mealbox_selection
        , 'Preselected Menu' as premium_dish
        , 'Preselected Menu' as thrifty_dish

    union all

    select
        2 as mealbox_composition_id
        , 'Subscription' as order_type_name
        , 'Customer Composed Menu' as mealbox_selection
        , 'Has Premium Dish' as premium_dish
        , 'Has Thrifty Dish' as thrifty_dish

    union all

    select
        3 as mealbox_composition_id
        , 'Subscription' as order_type_name
        , 'Customer Composed Menu' as mealbox_selection
        , 'No Premium Dish' as premium_dish
        , 'No Thrifty Dish' as thrifty_dish


    union all

    select
        4 as mealbox_composition_id
        , 'Subscription' as order_type_name
        , 'Customer Composed Menu' as mealbox_selection
        , 'Has Premium Dish' as premium_dish
        , 'No Thrifty Dish' as thrifty_dish

    union all

    select
        5 as mealbox_composition_id
        , 'Subscription' as order_type_name
        , 'Customer Composed Menu' as mealbox_selection
        , 'No Premium Dish' as premium_dish
        , 'Has Thrifty Dish' as thrifty_dish

)


, order_types_clean as (
    select
        order_type_id
        , case 
            when order_type_id in ({{var ('subscription_order_type_ids') | join(', ')}})
            then 'Subscription'
            else order_type_name
        end as order_type_name
        , is_direct_order
        , allows_anonymous
        , is_direct_payment
    from order_types

)

, all_tables_joined as (

    select 
        md5(concat_ws('-', order_type_id, mealbox_composition_id)) AS pk_dim_order_types
        , *
    from order_types_clean
    left join mealbox_composition
        using(order_type_name)

)

select * from all_tables_joined
