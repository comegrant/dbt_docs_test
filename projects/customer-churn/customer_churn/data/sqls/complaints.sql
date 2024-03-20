DECLARE @COMPANY_ID UNIQUEIDENTIFIER = '{company_id}'; -- '09ECD4F0-AE58-4539-8E8F-9275B1859A19';  -- GL

SELECT
        agreement_id
    ,	delivery_year
    ,	delivery_week
    ,	category
    ,	registration_date
    FROM mb.complaints
    WHERE delivery_year >= 2021
    AND company_id = @COMPANY_ID
