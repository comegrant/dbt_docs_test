
SELECT
        agreement_id
    ,	company_name
    ,	order_id
    ,	delivery_date
    ,	delivery_year
    ,	delivery_week
    ,	net_revenue_ex_vat
    ,	gross_revenue_ex_vat
    FROM dev.mltesting.mb_orders
    WHERE company_id =  '{company_id}'  -- '09ECD4F0-AE58-4539-8E8F-9275B1859A19' GL
    AND delivery_year >= 2021
