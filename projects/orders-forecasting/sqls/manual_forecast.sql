SELECT
    run_timestamp,
    year,
    week,
    sum(quantity) AS total_orders
FROM analytics.forecast_variations_history
WHERE
    product_type_id = '2f163d69-8ac1-6e0c-8793-ff0000804eb3'
    AND company_id = '{company_id}'
    AND model_id = 'bc9ccfbe-cfbc-49c3-a212-3f3490041b50'
GROUP BY
    run_timestamp,
    year,
    week
