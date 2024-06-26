SELECT
    estimation_timestamp,
    year,
    week,
    company_id,
    product_type_id,
    quantity_estimated_total
FROM {catalog_name}.{catalog_scheme}.estimations_total
WHERE company_id = '{company_id}'
