--This is the dataset for Batch API Quarantine
-- This extends the logic to work for Target week(for LMK), target week-1 (for GLAM), target week-2 (for RN)
-- This adjustment was reqd because CMS sends different input weeks for batch job for different companies


--Step 1: Fetch customer orders from past 4 weeks
DROP TABLE IF EXISTS #temp_cust_orders
select --top 5 --top 5 *
	ba.agreement_id
,	ba.status
,	ba.company_id
, 	o.company_name
,	o.order_id
,	o.order_creation_date
,	o.delivery_date
,	o.delivery_week
,	o.delivery_year
, 	op.variation_id
,	op.variation_name
,	op.product_id
,	op.product_name
,	op.product_type
,	op.product_type_id
into #temp_cust_orders
from mb.orders o
inner join cms.billing_agreement ba on ba.agreement_id = o.agreement_id
inner join mb.order_products op on op.order_id = o.order_id
where ba.company_id =@company_id
 and ba.status = 10
and o.delivery_date -->= (select dateadd(week, -4, '2023-12-05'))
    between dateadd(week, -4, dateadd(day, 1, dbo.find_first_day_of_week(datepart(year, getdate()), datepart(week,getdate())))) --automatic find 4 week back monday of year and week
    --and dbo.find_first_day_of_week(datepart(year, getdate()), datepart(week,getdate()))
    and dateadd(day, 1, dbo.find_first_day_of_week(datepart(year, getdate()), datepart(week,getdate()))) --modifying to fetch monday current week
and op.product_type_id IN ('CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1', --standalone dishes/velgvrak
						'288ED8CA-B437-4A6D-BE16-8F9A30185008') -- mealbox type subscription (Financial type)
order by ba.agreement_id, o.order_id



--Step 2: Fetch recipes in the orders
drop table if exists #temp_cust_orders_recipes
select
    tco.agreement_id
,	tco.status
,	tco.company_id
, 	tco.company_name
,	tco.order_id
,	tco.order_creation_date
,	tco.delivery_date
,	tco.delivery_week
,	tco.delivery_year
, 	tco.variation_id
,	tco.variation_name
,	tco.product_id
,	tco.product_name
,	tco.product_type
,   tco.product_type_id
,   mr.recipe_id
,   rmt.recipe_name
,   r2.recipe_id as pim_recipe_id
,   r2.main_recipe_id --extra
into #temp_cust_orders_recipes
from #temp_cust_orders tco
inner join cms.company c ON c.id = tco.company_id
inner join cms.country cont ON cont.id = c.country_id
inner join product_layer.product p ON p.id = tco.product_id
inner join pim.weekly_menus wm
    on wm.menu_week = tco.delivery_week
    and wm.menu_year = tco.delivery_year
    and wm.company_id = tco.company_id
inner join pim.MENUS m on m.MENU_NAME = p.name and wm.weekly_menus_id = m.WEEKLY_MENUS_ID
inner join pim.MENU_RECIPES mr on mr.MENU_ID = m.MENU_ID
inner join pim.recipes r2 on r2.recipe_id = mr.RECIPE_ID
inner join pim.recipe_metadata_translations rmt on rmt.recipe_metadata_id = r2.recipe_metadata_id AND rmt.language_id = cont.default_language_id



--Step 3: Fetch top 20 recommended dish for a week 5 weeks from now, 5 weeks being the week Mealselector Batch inserts into
DROP TABLE IF EXISTS #temp_all_recs_recipes
select
    DISTINCT
    r.agreement_id
,   r.week
,   r.year
,   r.product_id
,   r.company_id
,   p.name
,   rmt.recipe_name
,   wm.weekly_menus_id
,   mr.recipe_id
,   r2.main_recipe_id
,   r.order_of_relevance_cluster
INTO #temp_all_recs_recipes
from ml_output.latest_recommendations r
--Fetching correct language id we need to join on country and cmpany
inner join cms.company c ON c.id = r.company_id
inner join cms.country cont ON cont.id = c.country_id
--Limit to active customers
inner join cms.billing_agreement ba on ba.agreement_id = r.agreement_id and ba.status = 10
inner join product_layer.product p ON p.id = r.product_id
inner join pim.weekly_menus wm on wm.menu_week = r.week and wm.menu_year = r.year and wm.company_id = r.company_id
inner join pim.MENUS m on m.MENU_NAME = p.name and wm.weekly_menus_id = m.WEEKLY_MENUS_ID
inner join pim.MENU_RECIPES mr on mr.MENU_ID = m.MENU_ID
inner join pim.recipes r2 on r2.recipe_id = mr.RECIPE_ID
inner join pim.recipe_metadata_translations rmt on rmt.recipe_metadata_id = r2.recipe_metadata_id AND rmt.language_id = cont.default_language_id
--Specify company and week from latest data available
where r.company_id = @company_id--'09ECD4F0-AE58-4539-8E8F-9275B1859A19'--'5E65A955-7B1A-446C-B24F-CFE576BF52D7'--'8A613C15-35E4-471F-91CC-972F933331D7'
and r.order_of_relevance_cluster <= 20
and r.week = (select datepart(iso_week,dateadd(week, 6,  getdate() ))) --modifying this to 6 from 5 originally because due to batch job date, which gets shifted to +1wk, this needs to change here as well



-- Step 4: Return recommended recipes not in the customer's orders for the past 4weeks
DROP TABLE IF EXISTS #temp_recs_recipes_to_send
SELECT
    DISTINCT
    tarr.agreement_id
,   tarr.main_recipe_id
,   tarr.recipe_name
,   tarr.product_id
,   tarr.company_id
,   tarr.order_of_relevance_cluster
--,    tcor.delivery_week
,   tarr.week as recommended_week
,   tarr.year
INTO #temp_recs_recipes_to_send
FROM #temp_all_recs_recipes tarr --tbl1
LEFT JOIN #temp_cust_orders_recipes tcor --tbl2
ON tcor.agreement_id = tarr.agreement_id and tcor.main_recipe_id = tarr.main_recipe_id
where tcor.main_recipe_id is NULL



---------->>>>>>>>>>>>>>>>>>>>>>>>>>>____________________<<<<<<<<Fetching deviation info for customers>>>>>>>>>>>>____________________<<<<<<<<<<<<<<<<<<<<<<<<<<----------------------------------

DECLARE @week INT, @year INT
SET @week = (select datepart(iso_week,dateadd(week, 5,  getdate() ))) -->correct syntax for fetching +4weks from future <- goes in filter for fetching DEVIATIONS --changing this also for LMK due to batch job run day
SET @year = (select datepart(year,dateadd(week, 5,  getdate() )))
--select @week, @year


-- Step 5: : Fetch all week deviations
DROP TABLE IF EXISTS #week_deviation, #default_products, #default_dishes, #agreement_default_dishes_week
SELECT
    ba.agreement_id
    , ba.company_id
    , babd.year
    , babd.week
    , babd.updated_at
    , babd.created_by
    , babd.origin
    , babdp.subscribed_product_variation_id as variation_id
    , babdp.quantity
INTO #week_deviation
FROM cms.billing_agreement ba
INNER JOIN cms.billing_agreement_basket bab ON bab.agreement_id = ba.agreement_id
INNER JOIN cms.billing_agreement_basket_deviation babd ON babd.billing_agreement_basket_id = bab.id
	AND babd.week = @week
    AND babd.year = @year AND babd.is_active = 1
INNER JOIN cms.billing_agreement_basket_deviation_product babdp ON babdp.billing_agreement_basket_deviation_id = babd.id

-- Step 6: Get default dishes
    SELECT
        wm.menu_year
        , wm.menu_week
        , wm.company_id
        , mv.menu_variation_ext_id as variation_id
        , mv.variation_name
        , mr.recipe_id
        , mr.menu_recipe_order
    INTO #default_dishes
    FROM pim.weekly_menus wm
    INNER JOIN pim.menus m ON m.weekly_menus_id = wm.weekly_menus_id AND m.PRODUCT_TYPE_ID = '2F163D69-8AC1-6E0C-8793-FF0000804EB3'
    INNER JOIN pim.menu_variations mv ON mv.menu_id = m.menu_id
    INNER JOIN pim.menu_recipes mr ON mr.menu_id = m.menu_id AND mr.menu_recipe_order <= mv.menu_number_days
    WHERE wm.menu_year = @year
        AND wm.menu_week = @week
        AND wm.COMPANY_ID = @company_id

-- Step 7: Get default dishes for agreement product (also double checking that it matches concept)
    SELECT
        ba.agreement_id
        , pvc.name
        , dd.recipe_id
        , dd.menu_recipe_order
    INTO #agreement_default_dishes_week
    FROM cms.billing_agreement ba
    INNER JOIN cms.billing_agreement_basket bab ON bab.agreement_id = ba.agreement_id
    INNER JOIN cms.billing_agreement_basket_product babp ON babp.billing_agreement_basket_id = bab.id
    INNER JOIN product_layer.product_variation pv ON pv.id = babp.subscribed_product_variation_id
    INNER JOIN product_layer.product p ON p.id = pv.product_id
    INNER JOIN product_layer.product_variation_company pvc ON pvc.variation_id = pv.id AND pvc.company_id = ba.company_id
    INNER JOIN cms.billing_agreement_preference bap ON bap.agreement_id = ba.agreement_id
    INNER JOIN cms.preference pref ON pref.preference_id = bap.preference_id
    	AND pref.preference_type_id = '009CF63E-6E84-446C-9CE4-AFDBB6BB9687' -- concept
    INNER JOIN cms.preference_attribute_value pav ON pav.attribute_value = pv.product_id
    	AND pav.attribute_id = '680D1B8A-F967-4713-A270-8E30D683F4B8' -- linked_mealbox_id
    	AND pav.preference_id = pref.preference_id
    INNER JOIN #default_dishes dd ON dd.variation_id = babp.subscribed_product_variation_id

-- Step 8: Fetch the default dishes + deviation dishes
DROP TABLE IF EXISTS #temp_all_cust_mealselector_deviations
SELECT DISTINCT --top 15
    wd.agreement_id
    , wd.year
    , wd.week
    , wd.updated_at
    , wd.created_by
    , p.PRODUCT_TYPE_ID
    , p.name as product
    , pvc.name as variation
    , wd.quantity
    , r.recipe_name
    , r.main_recipe_id --extra
    , addw.menu_recipe_order
INTO #temp_all_cust_mealselector_deviations
FROM #week_deviation wd
LEFT JOIN ( --Fetches recipe name in this secion below
	SELECT
        wm.company_id
        , r.recipe_id
        , r.main_recipe_id -- extra
        , rmt.recipe_name
        , m.menu_name
        , mv.variation_name
        , mv.menu_variation_ext_id as variation_id
	FROM pim.WEEKLY_MENUS wm
	INNER JOIN cms.COMPANY c ON c.id = wm.company_id
	INNER JOIN cms.COUNTRY co ON co.id = c.country_id
	INNER JOIN pim.MENUS m ON m.weekly_menus_id = wm.weekly_menus_id AND m.PRODUCT_TYPE_ID = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1'
	INNER JOIN pim.MENU_VARIATIONS mv ON mv.menu_id = m.menu_id
	INNER JOIN pim.MENU_RECIPES mr ON m.menu_id = mr.menu_id and mr.menu_recipe_order <= mv.menu_number_days
	INNER JOIN pim.RECIPES r ON r.recipe_id = mr.recipe_id
	INNER JOIN pim.RECIPE_PORTIONS rp ON rp.RECIPE_ID = r.recipe_id AND rp.PORTION_ID = mv.PORTION_ID
	INNER JOIN pim.recipes_metadata rm ON rm.recipe_metadata_id = r.recipe_metadata_id
	INNER JOIN pim.RECIPE_METADATA_TRANSLATIONS rmt ON rmt.recipe_metadata_id = r.recipe_metadata_id and rmt.language_id = co.default_language_id
	WHERE
        wm.menu_year = @year
        AND wm.menu_week = @week
) r ON r.company_id = wd.company_id AND r.variation_id = wd.variation_id
LEFT JOIN #agreement_default_dishes_week addw ON addw.recipe_id = r.recipe_id
INNER JOIN product_layer.product_variation_company pvc ON pvc.variation_id = wd.variation_id AND pvc.company_id = wd.company_id
INNER JOIN product_layer.product_variation pv ON pv.id = pvc.variation_id
INNER JOIN product_layer.product p ON p.id = pv.product_id
WHERE wd.origin = '9E016A92-9E5C-4B5B-AC5D-739CEFD6F07B' -- actually mealselector but appears as rec engine/rec engine batch
and p.product_type_id = 'cac333ea-ec15-4eea-9d8d-2b9ef60ec0c1' --fetch only dishes and not the mealbox type
order by wd.agreement_id


-- Step 9: Return recommended recipes not in the customer's orders for the past 4weeks + not in deviations from target week -1 i.e. 4th week from present week
DROP TABLE IF EXISTS #temp_final_recs_recipes_to_send
SELECT
    DISTINCT
    trrts.agreement_id
,   trrts.main_recipe_id
,   trrts.recipe_name
,   trrts.product_id
,   trrts.company_id
,   trrts.order_of_relevance_cluster
--,    tcor.delivery_week
,   trrts.recommended_week
,   trrts.year
INTO #temp_final_recs_recipes_to_send
FROM #temp_recs_recipes_to_send trrts --tbl1
LEFT JOIN #temp_all_cust_mealselector_deviations tacmd --tbl2
ON tacmd.agreement_id = trrts.agreement_id and tacmd.main_recipe_id = trrts.main_recipe_id
where tacmd.main_recipe_id is NULL
ORDER BY agreement_id, order_of_relevance_cluster ASC

---------------------!!!!!!!!!!!!!>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>               Target week -1    for GLAM            <<<<<<<<<<<<<<<<<<<<<<<<<<!!!!!!!!!!!!!!!!------------------------------------
--Step 1: Fetch customer orders from past 4 weeks
DROP TABLE IF EXISTS #temp_cust_orders1
select --top 5 --top 5 *
	ba.agreement_id
,	ba.status
,	ba.company_id
, 	o.company_name
,	o.order_id
,	o.order_creation_date
,	o.delivery_date
,	o.delivery_week
,	o.delivery_year
, 	op.variation_id
,	op.variation_name
,	op.product_id
,	op.product_name
,	op.product_type
,	op.product_type_id
into #temp_cust_orders1
from mb.orders o
inner join cms.billing_agreement ba on ba.agreement_id = o.agreement_id
inner join mb.order_products op on op.order_id = o.order_id
where ba.company_id =@company_id
 and ba.status = 10
and o.delivery_date -->= (select dateadd(week, -4, '2023-12-05'))
    between dateadd(week, -4, dateadd(day, 1, dbo.find_first_day_of_week(datepart(year, getdate()), datepart(week,getdate())))) --automatic find 4 week back monday of year and week
    --and dbo.find_first_day_of_week(datepart(year, getdate()), datepart(week,getdate()))
    and dateadd(day, 1, dbo.find_first_day_of_week(datepart(year, getdate()), datepart(week,getdate()))) --modifying to fetch monday current week
and op.product_type_id IN ('CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1', --standalone dishes/velgvrak
						'288ED8CA-B437-4A6D-BE16-8F9A30185008') -- mealbox type subscription (Financial type)
order by ba.agreement_id, o.order_id

--Step 2: Fetch recipes in the orders
drop table if exists #temp_cust_orders_recipes1
select
    tco.agreement_id
,	tco.status
,	tco.company_id
, 	tco.company_name
,	tco.order_id
,	tco.order_creation_date
,	tco.delivery_date
,	tco.delivery_week
,	tco.delivery_year
, 	tco.variation_id
,	tco.variation_name
,	tco.product_id
,	tco.product_name
,	tco.product_type
,   tco.product_type_id
,   mr.recipe_id
,   rmt.recipe_name
,   r2.recipe_id as pim_recipe_id
,   r2.main_recipe_id --extra
into #temp_cust_orders_recipes1
from #temp_cust_orders1 tco
inner join cms.company c ON c.id = tco.company_id
inner join cms.country cont ON cont.id = c.country_id
inner join product_layer.product p ON p.id = tco.product_id
inner join pim.weekly_menus wm
    on wm.menu_week = tco.delivery_week
    and wm.menu_year = tco.delivery_year
    and wm.company_id = tco.company_id
inner join pim.MENUS m on m.MENU_NAME = p.name and wm.weekly_menus_id = m.WEEKLY_MENUS_ID
inner join pim.MENU_RECIPES mr on mr.MENU_ID = m.MENU_ID
inner join pim.recipes r2 on r2.recipe_id = mr.RECIPE_ID
inner join pim.recipe_metadata_translations rmt on rmt.recipe_metadata_id = r2.recipe_metadata_id AND rmt.language_id = cont.default_language_id

--Step 3: Fetch top 20 recommended dish for a week 5 weeks from now, 5 weeks being the week Mealselector Batch inserts into
DROP TABLE IF EXISTS #temp_all_recs_recipes1
select
    DISTINCT
    r.agreement_id
,   r.week
,   r.year
,   r.product_id
,   r.company_id
,   p.name
,   rmt.recipe_name
,   wm.weekly_menus_id
,   mr.recipe_id
,   r2.main_recipe_id
,   r.order_of_relevance_cluster
INTO #temp_all_recs_recipes1
from ml_output.latest_recommendations r
--Fetching correct language id we need to join on country and cmpany
inner join cms.company c ON c.id = r.company_id
inner join cms.country cont ON cont.id = c.country_id
--Limit to active customers
inner join cms.billing_agreement ba on ba.agreement_id = r.agreement_id and ba.status = 10
inner join product_layer.product p ON p.id = r.product_id
inner join pim.weekly_menus wm on wm.menu_week = r.week and wm.menu_year = r.year and wm.company_id = r.company_id
inner join pim.MENUS m on m.MENU_NAME = p.name and wm.weekly_menus_id = m.WEEKLY_MENUS_ID
inner join pim.MENU_RECIPES mr on mr.MENU_ID = m.MENU_ID
inner join pim.recipes r2 on r2.recipe_id = mr.RECIPE_ID
inner join pim.recipe_metadata_translations rmt on rmt.recipe_metadata_id = r2.recipe_metadata_id AND rmt.language_id = cont.default_language_id
--Specify company and week from latest data available
where r.company_id = @company_id
and r.order_of_relevance_cluster <= 20
and r.week = (select datepart(iso_week,dateadd(week, 5,  getdate() ))) --Changing to CORRECT 5 weeks from now for GLAM companies



-- Step 4: Return recommended recipes not in the customer's orders for the past 4weeks
DROP TABLE IF EXISTS #temp_recs_recipes_to_send1
SELECT
    DISTINCT
    tarr.agreement_id
,   tarr.main_recipe_id
,   tarr.recipe_name
,   tarr.product_id
,   tarr.company_id
,   tarr.order_of_relevance_cluster
--,    tcor.delivery_week
,   tarr.week as recommended_week
,   tarr.year
INTO #temp_recs_recipes_to_send1
FROM #temp_all_recs_recipes1 tarr --tbl1
LEFT JOIN #temp_cust_orders_recipes1 tcor --tbl2
ON tcor.agreement_id = tarr.agreement_id and tcor.main_recipe_id = tarr.main_recipe_id
where tcor.main_recipe_id is NULL


---------->>>>>>>>>>>>>>>>>>>>>>>>>>>____________________<<<<<<<<Fetching deviation info for customers for target week-1 >>>>>>>>>>>>____________________<<<<<<<<<<<<<<<<<<<<<<<<<<----------------------------------

DECLARE @week1 INT, @year1 INT
SET @week1 = (select datepart(iso_week,dateadd(week, 4,  getdate() ))) -->correct syntax for fetching +3weks from future <- goes in filter for fetching DEVIATIONS--correct to 4 WEEKS
SET @year1 = (select datepart(year,dateadd(week, 4,  getdate() )))


-- Step 5: : Fetch all week deviations
DROP TABLE IF EXISTS #week_deviation1, #default_products1, #default_dishes1, #agreement_default_dishes_week1
SELECT
    ba.agreement_id
    , ba.company_id
    , babd.year
    , babd.week
    , babd.updated_at
    , babd.created_by
    , babd.origin
    , babdp.subscribed_product_variation_id as variation_id
    , babdp.quantity
INTO #week_deviation1
FROM cms.billing_agreement ba
INNER JOIN cms.billing_agreement_basket bab ON bab.agreement_id = ba.agreement_id
INNER JOIN cms.billing_agreement_basket_deviation babd ON babd.billing_agreement_basket_id = bab.id
	AND babd.week = @week1
    AND babd.year = @year1 AND babd.is_active = 1
INNER JOIN cms.billing_agreement_basket_deviation_product babdp ON babdp.billing_agreement_basket_deviation_id = babd.id

-- Step 6: Get default dishes
    SELECT
        wm.menu_year
        , wm.menu_week
        , wm.company_id
        , mv.menu_variation_ext_id as variation_id
        , mv.variation_name
        , mr.recipe_id
        , mr.menu_recipe_order
    INTO #default_dishes1
    FROM pim.weekly_menus wm
    INNER JOIN pim.menus m ON m.weekly_menus_id = wm.weekly_menus_id AND m.PRODUCT_TYPE_ID = '2F163D69-8AC1-6E0C-8793-FF0000804EB3'
    INNER JOIN pim.menu_variations mv ON mv.menu_id = m.menu_id
    INNER JOIN pim.menu_recipes mr ON mr.menu_id = m.menu_id AND mr.menu_recipe_order <= mv.menu_number_days
    WHERE wm.menu_year = @year1
        AND wm.menu_week = @week1
        AND wm.COMPANY_ID = @company_id

-- Step 7: Get default dishes for agreement product (also double checking that it matches concept)
    SELECT
        ba.agreement_id
        , pvc.name
        , dd.recipe_id
        , dd.menu_recipe_order
    INTO #agreement_default_dishes_week1
    FROM cms.billing_agreement ba
    INNER JOIN cms.billing_agreement_basket bab ON bab.agreement_id = ba.agreement_id
    INNER JOIN cms.billing_agreement_basket_product babp ON babp.billing_agreement_basket_id = bab.id
    INNER JOIN product_layer.product_variation pv ON pv.id = babp.subscribed_product_variation_id
    INNER JOIN product_layer.product p ON p.id = pv.product_id
    INNER JOIN product_layer.product_variation_company pvc ON pvc.variation_id = pv.id AND pvc.company_id = ba.company_id
    INNER JOIN cms.billing_agreement_preference bap ON bap.agreement_id = ba.agreement_id
    INNER JOIN cms.preference pref ON pref.preference_id = bap.preference_id
    	AND pref.preference_type_id = '009CF63E-6E84-446C-9CE4-AFDBB6BB9687' -- concept
    INNER JOIN cms.preference_attribute_value pav ON pav.attribute_value = pv.product_id
    	AND pav.attribute_id = '680D1B8A-F967-4713-A270-8E30D683F4B8' -- linked_mealbox_id
    	AND pav.preference_id = pref.preference_id
    INNER JOIN #default_dishes1 dd ON dd.variation_id = babp.subscribed_product_variation_id

-- Step 8: Fetch the default dishes + deviation dishes
DROP TABLE IF EXISTS #temp_all_cust_mealselector_deviations1
SELECT DISTINCT --top 15
    wd.agreement_id
    , wd.year
    , wd.week
    , wd.updated_at
    , wd.created_by
    , p.PRODUCT_TYPE_ID
    , p.name as product
    , pvc.name as variation
    , wd.quantity
    , r.recipe_name
    , r.main_recipe_id --extra
    , addw.menu_recipe_order
INTO #temp_all_cust_mealselector_deviations1
FROM #week_deviation1 wd
LEFT JOIN ( --Fetches recipe name in this secion below
	SELECT
        wm.company_id
        , r.recipe_id
        , r.main_recipe_id -- extra
        , rmt.recipe_name
        , m.menu_name
        , mv.variation_name
        , mv.menu_variation_ext_id as variation_id
	FROM pim.WEEKLY_MENUS wm
	INNER JOIN cms.COMPANY c ON c.id = wm.company_id
	INNER JOIN cms.COUNTRY co ON co.id = c.country_id
	INNER JOIN pim.MENUS m ON m.weekly_menus_id = wm.weekly_menus_id AND m.PRODUCT_TYPE_ID = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1'
	INNER JOIN pim.MENU_VARIATIONS mv ON mv.menu_id = m.menu_id
	INNER JOIN pim.MENU_RECIPES mr ON m.menu_id = mr.menu_id and mr.menu_recipe_order <= mv.menu_number_days
	INNER JOIN pim.RECIPES r ON r.recipe_id = mr.recipe_id
	INNER JOIN pim.RECIPE_PORTIONS rp ON rp.RECIPE_ID = r.recipe_id AND rp.PORTION_ID = mv.PORTION_ID
	INNER JOIN pim.recipes_metadata rm ON rm.recipe_metadata_id = r.recipe_metadata_id
	INNER JOIN pim.RECIPE_METADATA_TRANSLATIONS rmt ON rmt.recipe_metadata_id = r.recipe_metadata_id and rmt.language_id = co.default_language_id
	WHERE
        wm.menu_year = @year1
        AND wm.menu_week = @week1
) r ON r.company_id = wd.company_id AND r.variation_id = wd.variation_id
LEFT JOIN #agreement_default_dishes_week1 addw ON addw.recipe_id = r.recipe_id
INNER JOIN product_layer.product_variation_company pvc ON pvc.variation_id = wd.variation_id AND pvc.company_id = wd.company_id
INNER JOIN product_layer.product_variation pv ON pv.id = pvc.variation_id
INNER JOIN product_layer.product p ON p.id = pv.product_id
WHERE wd.origin = '9E016A92-9E5C-4B5B-AC5D-739CEFD6F07B' -- actually mealselector but appears as rec engine/rec engine batch
and p.product_type_id = 'cac333ea-ec15-4eea-9d8d-2b9ef60ec0c1' --fetch only dishes and not the mealbox type
order by wd.agreement_id

-- Step 9: Return recommended recipes not in the customer's orders for the past 4weeks + not in deviations from target week -1 i.e. 3rd week from present week
DROP TABLE IF EXISTS #temp_final_recs_recipes_to_send1
SELECT
    DISTINCT
    trrts.agreement_id
,   trrts.main_recipe_id
,   trrts.recipe_name
,   trrts.product_id
,   trrts.company_id
,   trrts.order_of_relevance_cluster
--,    tcor.delivery_week
,   trrts.recommended_week
,   trrts.year
INTO #temp_final_recs_recipes_to_send1
FROM #temp_recs_recipes_to_send1 trrts --tbl1
LEFT JOIN #temp_all_cust_mealselector_deviations1 tacmd --tbl2
ON tacmd.agreement_id = trrts.agreement_id and tacmd.main_recipe_id = trrts.main_recipe_id
where tacmd.main_recipe_id is NULL
ORDER BY agreement_id, order_of_relevance_cluster ASC

---------------------!!!!!!!!!!!!!>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>               Target week -2 for RN                    <<<<<<<<<<<<<<<<<<<<<<<<<<!!!!!!!!!!!!!!!!------------------------------------
--Step 1: Fetch customer orders from past 4 weeks
DROP TABLE IF EXISTS #temp_cust_orders2
select --top 5 --top 5 *
	ba.agreement_id
,	ba.status
,	ba.company_id
, 	o.company_name
,	o.order_id
,	o.order_creation_date
,	o.delivery_date
,	o.delivery_week
,	o.delivery_year
, 	op.variation_id
,	op.variation_name
,	op.product_id
,	op.product_name
,	op.product_type
,	op.product_type_id
into #temp_cust_orders2
from mb.orders o
inner join cms.billing_agreement ba on ba.agreement_id = o.agreement_id
inner join mb.order_products op on op.order_id = o.order_id
where ba.company_id =@company_id
 and ba.status = 10
and o.delivery_date -->= (select dateadd(week, -4, '2023-12-05'))
    between dateadd(week, -4, dateadd(day, 1, dbo.find_first_day_of_week(datepart(year, getdate()), datepart(week,getdate())))) --automatic find 4 week back monday of year and week
    --and dbo.find_first_day_of_week(datepart(year, getdate()), datepart(week,getdate()))
    and dateadd(day, 1, dbo.find_first_day_of_week(datepart(year, getdate()), datepart(week,getdate()))) --modifying to fetch monday current week
and op.product_type_id IN ('CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1', --standalone dishes/velgvrak
						'288ED8CA-B437-4A6D-BE16-8F9A30185008') -- mealbox type subscription (Financial type)
order by ba.agreement_id, o.order_id

--Step 2: Fetch recipes in the orders
drop table if exists #temp_cust_orders_recipes2
select
    tco.agreement_id
,	tco.status
,	tco.company_id
, 	tco.company_name
,	tco.order_id
,	tco.order_creation_date
,	tco.delivery_date
,	tco.delivery_week
,	tco.delivery_year
, 	tco.variation_id
,	tco.variation_name
,	tco.product_id
,	tco.product_name
,	tco.product_type
,   tco.product_type_id
,   mr.recipe_id
,   rmt.recipe_name
,   r2.recipe_id as pim_recipe_id
,   r2.main_recipe_id --extra
into #temp_cust_orders_recipes2
from #temp_cust_orders2 tco
inner join cms.company c ON c.id = tco.company_id
inner join cms.country cont ON cont.id = c.country_id
inner join product_layer.product p ON p.id = tco.product_id
inner join pim.weekly_menus wm
    on wm.menu_week = tco.delivery_week
    and wm.menu_year = tco.delivery_year
    and wm.company_id = tco.company_id
inner join pim.MENUS m on m.MENU_NAME = p.name and wm.weekly_menus_id = m.WEEKLY_MENUS_ID
inner join pim.MENU_RECIPES mr on mr.MENU_ID = m.MENU_ID
inner join pim.recipes r2 on r2.recipe_id = mr.RECIPE_ID
inner join pim.recipe_metadata_translations rmt on rmt.recipe_metadata_id = r2.recipe_metadata_id AND rmt.language_id = cont.default_language_id

--Step 3: Fetch top 20 recommended dish for a week 5 weeks from now, 5 weeks being the week Mealselector Batch inserts into
DROP TABLE IF EXISTS #temp_all_recs_recipes2
select
    DISTINCT
    r.agreement_id
,   r.week
,   r.year
,   r.product_id
,   r.company_id
,   p.name
,   rmt.recipe_name
,   wm.weekly_menus_id
,   mr.recipe_id
,   r2.main_recipe_id
,   r.order_of_relevance_cluster
INTO #temp_all_recs_recipes2
from ml_output.latest_recommendations r
--Fetching correct language id we need to join on country and cmpany
inner join cms.company c ON c.id = r.company_id
inner join cms.country cont ON cont.id = c.country_id
--Limit to active customers
inner join cms.billing_agreement ba on ba.agreement_id = r.agreement_id and ba.status = 10
inner join product_layer.product p ON p.id = r.product_id
inner join pim.weekly_menus wm on wm.menu_week = r.week and wm.menu_year = r.year and wm.company_id = r.company_id
inner join pim.MENUS m on m.MENU_NAME = p.name and wm.weekly_menus_id = m.WEEKLY_MENUS_ID
inner join pim.MENU_RECIPES mr on mr.MENU_ID = m.MENU_ID
inner join pim.recipes r2 on r2.recipe_id = mr.RECIPE_ID
inner join pim.recipe_metadata_translations rmt on rmt.recipe_metadata_id = r2.recipe_metadata_id AND rmt.language_id = cont.default_language_id
--Specify company and week from latest data available
where r.company_id = @company_id
and r.order_of_relevance_cluster <= 20
and r.week = (select datepart(iso_week,dateadd(week, 4,  getdate() ))) --Instead of 5 weeks from now, fetch 3 weeks to account for target week-2 condn for RN --correcting to 4weeks in advance due to runtimes



-- Step 4: Return recommended recipes not in the customer's orders for the past 4weeks
DROP TABLE IF EXISTS #temp_recs_recipes_to_send2
SELECT
    DISTINCT
    tarr.agreement_id
,   tarr.main_recipe_id
,   tarr.recipe_name
,   tarr.product_id
,   tarr.company_id
,   tarr.order_of_relevance_cluster
--,    tcor.delivery_week
,   tarr.week as recommended_week
,   tarr.year
INTO #temp_recs_recipes_to_send2
FROM #temp_all_recs_recipes2 tarr --tbl1
LEFT JOIN #temp_cust_orders_recipes2 tcor --tbl2
ON tcor.agreement_id = tarr.agreement_id and tcor.main_recipe_id = tarr.main_recipe_id
where tcor.main_recipe_id is NULL


---------->>>>>>>>>>>>>>>>>>>>>>>>>>>____________________<<<<<<<<Fetching deviation info for customers for target week-2 for RN >>>>>>>>>>>>____________________<<<<<<<<<<<<<<<<<<<<<<<<<<----------------------------------

DECLARE @week2 INT, @year2 INT
SET @week2 = (select datepart(iso_week,dateadd(week, 3,  getdate() ))) -->correct syntax for fetching +2weks from future <- goes in filter for fetching DEVIATIONS --correcting to 3weeks due to runtimes
SET @year2 = (select datepart(year,dateadd(week, 3,  getdate() )))

-- Step 5: : Fetch all week deviations
DROP TABLE IF EXISTS #week_deviation2, #default_products2, #default_dishes2, #agreement_default_dishes_week2
SELECT
    ba.agreement_id
    , ba.company_id
    , babd.year
    , babd.week
    , babd.updated_at
    , babd.created_by
    , babd.origin
    , babdp.subscribed_product_variation_id as variation_id
    , babdp.quantity
INTO #week_deviation2
FROM cms.billing_agreement ba
INNER JOIN cms.billing_agreement_basket bab ON bab.agreement_id = ba.agreement_id
INNER JOIN cms.billing_agreement_basket_deviation babd ON babd.billing_agreement_basket_id = bab.id
	AND babd.week = @week2
    AND babd.year = @year2 AND babd.is_active = 1
INNER JOIN cms.billing_agreement_basket_deviation_product babdp ON babdp.billing_agreement_basket_deviation_id = babd.id

-- Step 6: Get default dishes
    SELECT
        wm.menu_year
        , wm.menu_week
        , wm.company_id
        , mv.menu_variation_ext_id as variation_id
        , mv.variation_name
        , mr.recipe_id
        , mr.menu_recipe_order
    INTO #default_dishes2
    FROM pim.weekly_menus wm
    INNER JOIN pim.menus m ON m.weekly_menus_id = wm.weekly_menus_id AND m.PRODUCT_TYPE_ID = '2F163D69-8AC1-6E0C-8793-FF0000804EB3'
    INNER JOIN pim.menu_variations mv ON mv.menu_id = m.menu_id
    INNER JOIN pim.menu_recipes mr ON mr.menu_id = m.menu_id AND mr.menu_recipe_order <= mv.menu_number_days
    WHERE wm.menu_year = @year2
        AND wm.menu_week = @week2
        AND wm.COMPANY_ID = @company_id

-- Step 7: Get default dishes for agreement product (also double checking that it matches concept)
    SELECT
        ba.agreement_id
        , pvc.name
        , dd.recipe_id
        , dd.menu_recipe_order
    INTO #agreement_default_dishes_week2
    FROM cms.billing_agreement ba
    INNER JOIN cms.billing_agreement_basket bab ON bab.agreement_id = ba.agreement_id
    INNER JOIN cms.billing_agreement_basket_product babp ON babp.billing_agreement_basket_id = bab.id
    INNER JOIN product_layer.product_variation pv ON pv.id = babp.subscribed_product_variation_id
    INNER JOIN product_layer.product p ON p.id = pv.product_id
    INNER JOIN product_layer.product_variation_company pvc ON pvc.variation_id = pv.id AND pvc.company_id = ba.company_id
    INNER JOIN cms.billing_agreement_preference bap ON bap.agreement_id = ba.agreement_id
    INNER JOIN cms.preference pref ON pref.preference_id = bap.preference_id
    	AND pref.preference_type_id = '009CF63E-6E84-446C-9CE4-AFDBB6BB9687' -- concept
    INNER JOIN cms.preference_attribute_value pav ON pav.attribute_value = pv.product_id
    	AND pav.attribute_id = '680D1B8A-F967-4713-A270-8E30D683F4B8' -- linked_mealbox_id
    	AND pav.preference_id = pref.preference_id
    INNER JOIN #default_dishes2 dd ON dd.variation_id = babp.subscribed_product_variation_id

-- Step 8: Fetch the default dishes + deviation dishes
DROP TABLE IF EXISTS #temp_all_cust_mealselector_deviations2
SELECT DISTINCT --top 15
    wd.agreement_id
    , wd.year
    , wd.week
    , wd.updated_at
    , wd.created_by
    , p.PRODUCT_TYPE_ID
    , p.name as product
    , pvc.name as variation
    , wd.quantity
    , r.recipe_name
    , r.main_recipe_id --extra
    , addw.menu_recipe_order
INTO #temp_all_cust_mealselector_deviations2
FROM #week_deviation2 wd
LEFT JOIN ( --Fetches recipe name in this secion below
	SELECT
        wm.company_id
        , r.recipe_id
        , r.main_recipe_id -- extra
        , rmt.recipe_name
        , m.menu_name
        , mv.variation_name
        , mv.menu_variation_ext_id as variation_id
	FROM pim.WEEKLY_MENUS wm
	INNER JOIN cms.COMPANY c ON c.id = wm.company_id
	INNER JOIN cms.COUNTRY co ON co.id = c.country_id
	INNER JOIN pim.MENUS m ON m.weekly_menus_id = wm.weekly_menus_id AND m.PRODUCT_TYPE_ID = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1'
	INNER JOIN pim.MENU_VARIATIONS mv ON mv.menu_id = m.menu_id
	INNER JOIN pim.MENU_RECIPES mr ON m.menu_id = mr.menu_id and mr.menu_recipe_order <= mv.menu_number_days
	INNER JOIN pim.RECIPES r ON r.recipe_id = mr.recipe_id
	INNER JOIN pim.RECIPE_PORTIONS rp ON rp.RECIPE_ID = r.recipe_id AND rp.PORTION_ID = mv.PORTION_ID
	INNER JOIN pim.recipes_metadata rm ON rm.recipe_metadata_id = r.recipe_metadata_id
	INNER JOIN pim.RECIPE_METADATA_TRANSLATIONS rmt ON rmt.recipe_metadata_id = r.recipe_metadata_id and rmt.language_id = co.default_language_id
	WHERE
        wm.menu_year = @year2
        AND wm.menu_week = @week2
) r ON r.company_id = wd.company_id AND r.variation_id = wd.variation_id
LEFT JOIN #agreement_default_dishes_week2 addw ON addw.recipe_id = r.recipe_id
INNER JOIN product_layer.product_variation_company pvc ON pvc.variation_id = wd.variation_id AND pvc.company_id = wd.company_id
INNER JOIN product_layer.product_variation pv ON pv.id = pvc.variation_id
INNER JOIN product_layer.product p ON p.id = pv.product_id
WHERE wd.origin = '9E016A92-9E5C-4B5B-AC5D-739CEFD6F07B' -- actually mealselector but appears as rec engine/rec engine batch
and p.product_type_id = 'cac333ea-ec15-4eea-9d8d-2b9ef60ec0c1' --fetch only dishes and not the mealbox type
order by wd.agreement_id

-- Step 9: Return recommended recipes not in the customer's orders for the past 4weeks + not in deviations from target week -1 i.e. 3rd week from present week
DROP TABLE IF EXISTS #temp_final_recs_recipes_to_send2
SELECT
    DISTINCT
    trrts.agreement_id
,   trrts.main_recipe_id
,   trrts.recipe_name
,   trrts.product_id
,   trrts.company_id
,   trrts.order_of_relevance_cluster
--,    tcor.delivery_week
,   trrts.recommended_week
,   trrts.year
INTO #temp_final_recs_recipes_to_send2
FROM #temp_recs_recipes_to_send2 trrts --tbl1
LEFT JOIN #temp_all_cust_mealselector_deviations2 tacmd --tbl2
ON tacmd.agreement_id = trrts.agreement_id and tacmd.main_recipe_id = trrts.main_recipe_id
where tacmd.main_recipe_id is NULL
ORDER BY agreement_id, order_of_relevance_cluster ASC

--Clear out table from previous week's data
DELETE FROM py_analytics.batch_preference_quarantine WHERE company_id=@company_id

--Insert into MLDB table
INSERT INTO py_analytics.batch_preference_quarantine
SELECT * FROM #temp_final_recs_recipes_to_send
UNION ALL
SELECT * FROM #temp_final_recs_recipes_to_send1
UNION ALL
SELECT * FROM #temp_final_recs_recipes_to_send2










SELECT
    r.recipe_id
    , r.main_recipe_id
    , rmt.recipe_name
    , m.menu_name
    , mv.variation_name
    , mv.menu_variation_ext_id as variation_id
FROM pim.WEEKLY_MENUS wm
INNER JOIN cms.COMPANY c ON c.id = wm.company_id
INNER JOIN cms.COUNTRY co ON co.id = c.country_id
INNER JOIN pim.MENUS m ON m.weekly_menus_id = wm.weekly_menus_id AND m.PRODUCT_TYPE_ID = 'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1'
INNER JOIN pim.MENU_VARIATIONS mv ON mv.menu_id = m.menu_id
INNER JOIN pim.MENU_RECIPES mr ON m.menu_id = mr.menu_id and mr.menu_recipe_order <= mv.menu_number_days
INNER JOIN pim.RECIPES r ON r.recipe_id = mr.recipe_id
INNER JOIN pim.RECIPE_PORTIONS rp ON rp.RECIPE_ID = r.recipe_id AND rp.PORTION_ID = mv.PORTION_ID
INNER JOIN pim.recipes_metadata rm ON rm.recipe_metadata_id = r.recipe_metadata_id
INNER JOIN pim.RECIPE_METADATA_TRANSLATIONS rmt ON rmt.recipe_metadata_id = r.recipe_metadata_id and rmt.language_id = co.default_language_id
WHERE
    wm.menu_year = @year
    AND wm.menu_week = @week
    AND wm.company_id = @company_id




SELECT
        wm.menu_year
        , wm.menu_week
        , wm.company_id
        , mv.menu_variation_ext_id as variation_id
        , mv.variation_name
        , mr.recipe_id
        , mr.menu_recipe_order
    FROM pim.weekly_menus wm
    INNER JOIN pim.menus m ON m.weekly_menus_id = wm.weekly_menus_id AND m.PRODUCT_TYPE_ID = '2F163D69-8AC1-6E0C-8793-FF0000804EB3'
    INNER JOIN pim.menu_variations mv ON mv.menu_id = m.menu_id
    INNER JOIN pim.menu_recipes mr ON mr.menu_id = m.menu_id AND mr.menu_recipe_order <= mv.menu_number_days
    WHERE wm.menu_year = @year
        AND wm.menu_week = @week
        AND wm.COMPANY_ID = @company_id
