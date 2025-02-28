with active_freeze_billing_agreements as (
	select
	pk_dim_billing_agreements,
	billing_agreement_id,
	company_id,
	is_current
	from gold.dim_billing_agreements
	where billing_agreement_status_name in ('Active', 'Freezed')
	and company_id = '{company_id}'
),

billing_agreement_basket_products as (
	select
	fk_dim_billing_agreements,
	fk_dim_products,
	product_variation_id,
	product_variation_quantity
	from gold.bridge_billing_agreements_basket_products
),

dim_products as (
	select
	*
	from
	gold.dim_products
),

last_delivery_monday as (
	select
	billing_agreement_id,
	max(menu_week_monday_date) as last_delivery_monday
	from gold.fact_orders
	where order_status_id = '4508130E-6BA1-4C14-94A4-A56B074BB135' -- finished
	group by billing_agreement_id
),

days_since_last_delivery as (
	select
	billing_agreement_id,
	datediff(current_date(), last_delivery_monday) as days_since_last_delivery
	from last_delivery_monday
),

preferences as (
	select
	pk_dim_preference_combinations,
	fk_dim_billing_agreements,
	preference_id_combinations_concept_type as concept_preference_ids,
	preference_id_combinations_taste_type as taste_preference_ids
	from gold.dim_preference_combinations
),

valid_portions as (
	select distinct
	portion_quantity
	from gold.fact_menus
	where
	is_locked_recipe
	and menu_week_monday_date > current_date()
	and is_dish
	and has_recipe_portions
	and company_id = '{company_id}'

),

final as (
	select
	active_freeze_billing_agreements.billing_agreement_id as agreement_id,
	case
	when concept_preference_ids is not null then
		concat('["',
			concat_ws('", "',
				split(concept_preference_ids, ',')),
			'"]')
	else
		null
	end as concept_preference_ids,
	case
	when taste_preference_ids is not null then
		concat('["',
			concat_ws('", "',
				split(taste_preference_ids, ',')),
			'"]')
	else
		null
	end as taste_preference_ids,
	dim_products.meals as number_of_recipes,
	dim_products.portions as portion_size
	from
	active_freeze_billing_agreements
	left join
	preferences
	on active_freeze_billing_agreements.pk_dim_billing_agreements
	= preferences.fk_dim_billing_agreements
	left join
	billing_agreement_basket_products
	on active_freeze_billing_agreements.pk_dim_billing_agreements
	= billing_agreement_basket_products.fk_dim_billing_agreements
	left join
	dim_products
	on billing_agreement_basket_products.fk_dim_products = dim_products.pk_dim_products
	inner join
	days_since_last_delivery
	on active_freeze_billing_agreements.billing_agreement_id
	= days_since_last_delivery.billing_agreement_id
	where product_type_id = '2F163D69-8AC1-6E0C-8793-FF0000804EB3' -- Mealbox
	and days_since_last_delivery <= 12*7 -- 12 weeks
	and is_current = true
	and portions in (
		select portion_quantity from valid_portions
	)
)

select * from final where concept_preference_ids is not null
