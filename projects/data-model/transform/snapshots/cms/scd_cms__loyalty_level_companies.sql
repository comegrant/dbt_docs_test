{% snapshot scd_cms__loyalty_level_companies %}

select 
	id
	, level_id
	, company_id 
	, name
	, description
	, multiplier
	, requirement
	, created_at
	, created_by
	, updated_at
	, updated_by
	, retainable_level_id
	, retainable_points
	, multiplier_accrued
	, multiplier_spendable
	, name_en
from {{ source('cms', 'cms__loyalty_level_company') }}

{% endsnapshot %} 