-- Make sure that billing_agreement_preferences_updated_id in int_billing_agreement_preferences_unioned is unique.

select
    billing_agreement_preferences_updated_id
    , count(*) as count
from {{ ref('int_billing_agreement_preferences_unioned') }}
group by 1
having count(*) > 1
