declare @company_id uniqueidentifier = '{company_id}';
declare @status varchar(20) = '{status}';
declare @concept_preference_type_id uniqueidentifier = '{concept_preference_type_id}';
declare @taste_preference_type_id uniqueidentifier = '{taste_preference_type_id}';
DECLARE @PRODUCT_TYPE_ID_MEALBOX uniqueidentifier = '{product_type_id_mealbox}';

WITH concept_preferences AS (
    SELECT
        bap.agreement_id,
        STRING_AGG(convert(nvarchar(36), bap.preference_id), ', ') as preference_ids,
        STRING_AGG(pref.name, ', ') as preferences
    from cms.billing_agreement_preference bap
        JOIN cms.preference pref on pref.preference_id = bap.preference_id
        WHERE pref.preference_type_id = @concept_preference_type_id
    GROUP BY bap.agreement_id
),

taste_preferences AS (
    SELECT
        bap.agreement_id,
        STRING_AGG(convert(nvarchar(36), bap.preference_id), ', ') as preference_ids,
        STRING_AGG(pref.name, ', ') as preferences
    from cms.billing_agreement_preference bap
        JOIN cms.preference pref on pref.preference_id = bap.preference_id
        WHERE pref.preference_type_id = @taste_preference_type_id
    GROUP BY bap.agreement_id
)

SELECT ba.agreement_id
        , ba.company_id
        , ba.status
        , taste_pref.preference_ids taste_preference_ids
        , taste_pref.preferences taste_preference_names
        , concept_pref.preference_ids concept_preference_ids
        , concept_pref.preferences concept_preference_names
        , bap.subscribed_product_variation_id
        , bap.quantity
    FROM cms.billing_agreement ba
    LEFT JOIN taste_preferences taste_pref on taste_pref.agreement_id = ba.agreement_id
    LEFT JOIN concept_preferences concept_pref on concept_pref.agreement_id = ba.agreement_id
    LEFT JOIN cms.billing_agreement_basket bb on bb.agreement_id = ba.agreement_id
    LEFT JOIN cms.billing_agreement_basket_product bap on bap.billing_agreement_basket_id = bb.id
    INNER JOIN product_layer.product_variation pv ON pv.id = bap.subscribed_product_variation_id
    INNER JOIN product_layer.product p ON p.id = pv.product_id
    WHERE
        ba.status = @status
        AND company_id = @company_id
        AND p.product_type_id = @PRODUCT_TYPE_ID_MEALBOX
    order by agreement_id
