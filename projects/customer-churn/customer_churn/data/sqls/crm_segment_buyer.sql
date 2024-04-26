SELECT
        agreement_id
    ,	current_delivery_year
    ,	current_delivery_week
    ,	planned_delivery
    FROM dev.mltesting.analytics_crm_segment_agreement_main_latest
    WHERE current_delivery_year >= 2019
    AND company_id = '{company_id}' -- '09ECD4F0-AE58-4539-8E8F-9275B1859A19' --
    AND main_segment_name = 'Buyer'
