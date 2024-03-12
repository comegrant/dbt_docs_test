DECLARE @COMPANY_ID UNIQUEIDENTIFIER = '{company_id}'; -- '09ECD4F0-AE58-4539-8E8F-9275B1859A19';  -- GL

SELECT
    be.agreement_id,
    be.impulsiveness,
    be.created_at,
    be.cultural_class,
    be.perceived_purchasing_power,
    be.consumption,
    be.financial_class,
    be.life_stage,
    be.type_of_housing,
    be.confidence_level,
    be.probability_children_0_to_6,
    be.probability_children_7_to_17
    FROM bisnode.bisnode_enrichments be
    INNER JOIN cms.billing_agreement ba ON ba.agreement_id = be.agreement_id AND ba.company_id = @COMPANY_ID

    UNION ALL

    SELECT
    beh.agreement_id,
    beh.impulsiveness,
    beh.created_at,
    beh.cultural_class,
    beh.perceived_purchasing_power,
    beh.consumption,
    beh.financial_class,
    beh.life_stage,
    beh.type_of_housing,
    beh.confidence_level,
    beh.probability_children_0_to_6,
    beh.probability_children_7_to_17
    FROM bisnode.bisnode_enrichments_history beh
    INNER JOIN cms.billing_agreement ba ON ba.agreement_id = beh.agreement_id AND ba.company_id = @COMPANY_ID
