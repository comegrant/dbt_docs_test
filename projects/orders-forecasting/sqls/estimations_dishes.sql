SELECT
    estimation_timestamp,
    year,
    week,
    company_id,
    product_type_id,
    quantity_estimated_dishes
FROM
    {catalog_name}.{catalog_scheme}.estimations_dishes
WHERE company_id = '{company_id}'
ORDER BY
    year,
    week
