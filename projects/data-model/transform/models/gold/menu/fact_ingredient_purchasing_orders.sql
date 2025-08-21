with

weekly_purchase_orders as (

    select * from {{ ref('int_weekly_purchase_order_lines_joined') }}
)

, companies as (

    select * from {{ref('dim_companies')}}

)

, add_keys as (

    select
        md5(concat_ws('-',
            purchase_order_id
            , purchase_order_line_id
        )) as pk_fact_recipe_ingredients
        , purchase_order_id
        , purchase_order_line_id
        , weekly_purchase_orders.menu_year
        , weekly_purchase_orders.menu_week
        , weekly_purchase_orders.purchasing_company_id
        , weekly_purchase_orders.is_special_purchase_order
        , weekly_purchase_orders.production_date
        , weekly_purchase_orders.purchase_delivery_date
        , weekly_purchase_orders.ingredient_id
        , weekly_purchase_orders.original_ingredient_quantity
        , weekly_purchase_orders.ingredient_purchasing_cost
        , weekly_purchase_orders.vat
        , weekly_purchase_orders.total_purchasing_cost
        , weekly_purchase_orders.extra_ingredient_quantity 
        , weekly_purchase_orders.total_ingredient_quantity
        , weekly_purchase_orders.take_from_storage_ingredient_quantity
        , weekly_purchase_orders.received_ingredient_quantity
        , weekly_purchase_orders.ingredient_co2_emissions_per_unit
        , weekly_purchase_orders.total_ingredient_quantity * weekly_purchase_orders.ingredient_co2_emissions_per_unit as purchase_order_total_ingredient_quantity_co2_emissions
        , weekly_purchase_orders.received_ingredient_quantity * weekly_purchase_orders.ingredient_co2_emissions_per_unit as purchase_order_recieved_ingredient_quantity_co2_emissions

        {# FKS #}
        , cast(date_format(weekly_purchase_orders.menu_week_monday_date, 'yyyyMMdd') as int) as fk_dim_dates
        , md5(companies.company_id) as fk_dim_companies
        , md5(concat_ws( '-' , weekly_purchase_orders.ingredient_id, companies.language_id)) as fk_dim_ingredients

    from weekly_purchase_orders

    left join companies
        on weekly_purchase_orders.purchasing_company_id = companies.company_id

)

select * from add_keys