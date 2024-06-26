SELECT
    year,
    week,
    company_id,
    num_total_orders,
    num_mealbox_orders,
    num_dishes_orders,
    perc_dishes_orders
FROM
    {catalog_name}.{catalog_scheme}.orders_weekly_aggregated
WHERE company_id = '{company_id}'
ORDER BY
    year,
    week
