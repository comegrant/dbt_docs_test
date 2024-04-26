
SELECT
        agreement_id
    ,	agreement_status
    ,	agreement_start_date
    ,	agreement_start_year
    ,	agreement_start_week
    ,	agreement_first_delivery_year
    ,	agreement_first_delivery_week
    ,   agreement_creation_date
    ,   agreement_first_delivery_date
    ,   last_delivery_date
    ,   next_estimated_delivery_date

    FROM dev.mltesting.mb_customers
    WHERE company_id = '{company_id}' -- '09ECD4F0-AE58-4539-8E8F-9275B1859A19';  -- GL
    AND agreement_status_id != 40
