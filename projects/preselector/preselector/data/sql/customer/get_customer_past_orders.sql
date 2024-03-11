
declare @company_id uniqueidentifier = '{company_id}';
declare @week int = '{week}';
declare @year int = '{year}';

SELECT
    ba.agreement_id
,   bao.year as year
,   bao.week as week
,   r2.main_recipe_id
FROM cms.billing_agreement ba
INNER JOIN cms.billing_agreement_order bao ON bao.agreement_id = ba.agreement_id AND (
    bao.order_type IN (
            '5F34860B-7E61-46A0-80F7-98DCDC53BA9E' -- Recurring
        ,   '1C182E51-ECFA-4119-8928-F2D9F57C5FCC' -- Orders after cutoff
        ,   'C7D2684C-B715-4C6C-BF90-053757926679' -- Order After Registration
        ,   '7DB44860-1075-49A1-BF37-9B4D5B9C4BA4' -- Campaign
        ,   'C4C44413-78A7-4EB8-8715-2643D88849B6' -- Daily Direct Order
    )
    AND (
        bao.order_status_id IS NULL
        OR bao.order_status_id IN (
                '38A5CC25-A639-4433-A8E6-43FB35DABFD9' -- Processing
            ,   '4508130E-6BA1-4C14-94A4-A56B074BB135' -- Finished
        )
    )
)
INNER JOIN cms.company c ON c.id = ba.company_id
INNER JOIN cms.billing_agreement_order_line baol ON baol.agreement_order_id = bao.id
INNER JOIN product_layer.product_variation pv ON pv.id = baol.variation_id
INNER JOIN product_layer.product p ON pv.product_id = p.id AND p.product_type_id in (
    'CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1', -- Flex / Velg&Vrak
    '2F163D69-8AC1-6E0C-8793-FF0000804EB3' -- Mealbox
)
INNER JOIN product_layer.product_type pt ON pt.product_type_id = p.product_type_id
INNER JOIN pim.weekly_menus wm ON wm.menu_week = bao.week AND wm.menu_year = bao.year AND wm.company_id = ba.company_id
INNER JOIN pim.MENUS m ON m.MENU_NAME = p.name AND wm.weekly_menus_id = m.WEEKLY_MENUS_ID
INNER JOIN pim.MENU_RECIPES mr ON mr.MENU_ID = m.MENU_ID
INNER JOIN pim.recipes r2 ON r2.recipe_id = mr.RECIPE_ID
    WHERE bao.year = @year AND bao.week = @week AND ba.company_id = @company_id
