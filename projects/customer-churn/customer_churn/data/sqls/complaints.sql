SELECT
        agreement_id
    ,	delivery_year
    ,	delivery_week
    ,	category
    ,	registration_date
    FROM dev.mltesting.mb_complaints
    WHERE delivery_year >= 2021
    AND company_id = '{company_id}';
