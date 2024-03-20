DECLARE @COMPANY_ID UNIQUEIDENTIFIER = '{company_id}'; -- '09ECD4F0-AE58-4539-8E8F-9275B1859A19';  -- GL
DECLARE @YEAR_MIN INT = 2019;

SELECT
        agreement_id
    ,	current_delivery_year
    ,	current_delivery_week
    ,	planned_delivery
    FROM analytics.crm_segment_agreement_main_log
    WHERE current_delivery_year >= @YEAR_MIN
    AND company_id = @COMPANY_ID
    AND sub_segment_name != 'Deleted'   -- temporary as there are no events for deleted users
