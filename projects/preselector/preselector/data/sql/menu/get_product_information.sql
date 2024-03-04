declare @company uniqueidentifier = '{company_id}';

SELECT
    p.id AS product_id
,	p.name AS product_name
,	pt.product_type_name AS product_type
,   pt.product_type_id
,	pv.id AS variation_id
,	pv.sku AS variation_sku
,	pvc.name AS variation_name
,	pvc.company_id AS company_id
,	ISNULL(pvav.attribute_value, pvat.default_value) AS variation_meals
,	ISNULL(pvav2.attribute_value, pvat2.default_value) AS variation_portions
,	ISNULL(pvav4.attribute_value, pvat4.default_value) AS variation_price
,	ROUND(CAST(ISNULL(pvav4.attribute_value, pvat4.default_value) AS FLOAT) * (1.0 + (CAST(ISNULL(pvav5.attribute_value, pvat5.default_value) AS FLOAT) / 100)), 2) AS variation_price_incl_vat
,	ISNULL(pvav5.attribute_value, pvat5.default_value) AS variation_vat
FROM product_layer.product p
INNER JOIN product_layer.product_type pt ON pt.product_type_id = p.product_type_id
INNER JOIN product_layer.product_variation pv ON pv.product_id = p.id
INNER JOIN product_layer.product_variation_company pvc ON pvc.variation_id = pv.id
INNER JOIN cms.company c ON c.id = pvc.company_id

-- MEALS
LEFT JOIN product_layer.product_variation_attribute_template pvat ON pvat.product_type_id = p.product_type_id AND pvat.attribute_name = 'Meals'
LEFT JOIN product_layer.product_variation_attribute_value pvav ON pvav.attribute_id = pvat.attribute_id AND pvav.variation_id = pvc.variation_id AND pvav.company_id = pvc.company_id

-- PORTIONS
LEFT JOIN product_layer.product_variation_attribute_template pvat2 ON pvat2.product_type_id = p.product_type_id AND pvat2.attribute_name = 'Portions'
LEFT JOIN product_layer.product_variation_attribute_value pvav2 ON pvav2.attribute_id = pvat2.attribute_id AND pvav2.variation_id = pvc.variation_id AND pvav2.company_id = pvc.company_id

-- PRICE
LEFT JOIN product_layer.product_variation_attribute_template pvat4 ON pvat4.product_type_id = p.product_type_id AND pvat4.attribute_name = 'PRICE'
LEFT JOIN product_layer.product_variation_attribute_value pvav4 ON pvav4.attribute_id = pvat4.attribute_id AND pvav4.variation_id = pvc.variation_id AND pvav4.company_id = pvc.company_id

-- VAT
LEFT JOIN product_layer.product_variation_attribute_template pvat5 ON pvat5.product_type_id = p.product_type_id AND pvat5.attribute_name = 'VAT'
LEFT JOIN product_layer.product_variation_attribute_value pvav5 ON pvav5.attribute_id = pvat5.attribute_id AND pvav5.variation_id = pvc.variation_id AND pvav5.company_id = pvc.company_id

WHERE pvc.company_id=@company
