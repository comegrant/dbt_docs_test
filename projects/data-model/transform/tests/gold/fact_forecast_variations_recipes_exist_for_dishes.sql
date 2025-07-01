with forecast_variations as (

    select * from {{ ref('fact_forecast_variations') }}

),

products as (

    select * from {{ ref('dim_products') }}

)

select
    forecast_variations.*
from forecast_variations
left join products
    on fk_dim_products = pk_dim_products
where fk_dim_recipes is null
and product_type_id = '{{ var("velg&vrak_product_type_id") }}'
