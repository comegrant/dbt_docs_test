with 

ingredients as (

    select * from {{ ref('pim__ingredients') }}

)

, ingredient_translations as (

    select * from {{ ref('pim__ingredient_translations') }}

)

, unit_label_translations as (

    select * from {{ ref('pim__unit_label_translations') }}

)

, category_hierarchy as (

    select * from {{ ref('int_ingredient_category_hierarchies') }}

)

, ingredient_suppliers as (

    select * from {{ ref('pim__ingredient_suppliers') }}

)

, status_codes as (

    select * from {{ ref('pim__ingredient_status_codes') }}

)

, ingredient_types as (

    select * from {{ ref('pim__ingredient_types') }}

)

, ingredient_info as (

    select
        -- basic ids and names
        ingredients.ingredient_id
        , ingredients.ingredient_internal_reference
        , ingredient_translations.language_id
        , ingredient_translations.ingredient_name
        , concat(ingredient_translations.ingredient_name
            ,' '
            , ingredients.ingredient_size
            , unit_label_translations.unit_label_short_name
        ) as ingredient_full_name
        , unit_label_translations.unit_label_short_name
        , unit_label_translations.unit_label_full_name
        
        -- other ids
        , ingredients.ingredient_supplier_id
        , ingredients.unit_label_id
        , ingredients.ingredient_category_id
        , ingredients.ingredient_status_code_id --Add status description
        , ingredients.ingredient_type_id
        , ingredients.pack_type_id  --Add pack type name (N/A right now, needs PACK_TYPES_TRANSLATIONS table)
        , ingredients.epd_id_number
        , ingredients.ingredient_manufacturer_supplier_id
        
        -- strings
        , ingredients.ingredient_brand
        , ingredients.ingredient_content_list
        , ingredients.ean_code_consumer_packaging
        , ingredients.ean_code_distribution_packaging
        , ingredients.ingredient_external_reference
        , ingredient_suppliers.ingredient_supplier_name
        , ingredient_manufacturer_suppliers.ingredient_supplier_name as ingredient_manufacturer_name
        , status_codes.ingredient_status_name
        , ingredient_types.ingredient_type_name

        -- numerics
        , ingredients.ingredient_size
        , ingredients.ingredient_shelf_life
        , ingredients.ingredient_net_weight
        , ingredients.ingredient_gross_weight
        , ingredients.ingredient_distribution_packaging_size
        , ingredients.distribution_packages_per_pallet
        , ingredients.ingredient_packaging_depth
        , ingredients.ingredient_packaging_height
        , ingredients.ingredient_packaging_width

        -- booleans
        , ingredients.is_cold_storage
        , ingredients.is_consumer_cold_storage
        , ingredients.is_organic_ingredient
        , ingredients.is_fragile_ingredient
        , ingredients.is_special_packing
        , ingredients.has_customer_photo
        , ingredients.has_packaging_photo
        , ingredients.has_internal_photo
        , ingredients.is_to_register_batch_number
        , ingredients.is_to_register_expiration_date
        , ingredients.is_to_register_temperature
        , ingredients.is_to_register_ingredient_weight
        , ingredients.has_co2_data

    from ingredients
    
    left join ingredient_translations
        on ingredients.ingredient_id = ingredient_translations.ingredient_id

    left join unit_label_translations
        on ingredients.unit_label_id = unit_label_translations.unit_label_id
        and ingredient_translations.language_id = unit_label_translations.language_id

    left join ingredient_suppliers
        on ingredients.ingredient_supplier_id = ingredient_suppliers.ingredient_supplier_id

    left join ingredient_suppliers as ingredient_manufacturer_suppliers
        on ingredients.ingredient_manufacturer_supplier_id = ingredient_manufacturer_suppliers.ingredient_supplier_id

    left join status_codes
        on ingredients.ingredient_status_code_id = status_codes.ingredient_status_code_id

    left join ingredient_types
        on ingredients.ingredient_type_id = ingredient_types.ingredient_type_id

)

, group_name_extraction as (
    select
        ingredient_id
        , language_id
        , any_value(case when ingredient_category_description = 'Main Group' then ingredient_category_name end) ignore nulls as main_group_name
        , any_value(case when ingredient_category_description = 'Category Group' then ingredient_category_name end) ignore nulls as category_group_name
        , any_value(case when ingredient_category_description = 'Product Group' then ingredient_category_name end) ignore nulls as product_group_name
    from category_hierarchy
    group by 1, 2
)

, flat_hierarchy as (
    select
        ingredient_id
        , language_id
        , any_value(case when hierarchy_level_name = 'Main category' then ingredient_category_name end) ignore nulls as main_category_name
        , any_value(case when hierarchy_level_name = 'Sub category 1' then ingredient_category_name end) ignore nulls as sub_category1_name
        , any_value(case when hierarchy_level_name = 'Sub category 2' then ingredient_category_name end) ignore nulls as sub_category2_name
        , any_value(case when hierarchy_level_name = 'Sub category 3' then ingredient_category_name end) ignore nulls as sub_category3_name
        , any_value(case when hierarchy_level_name = 'Sub category 4' then ingredient_category_name end) ignore nulls as sub_category4_name
        , any_value(case when hierarchy_level = 0 then ingredient_category_id end) ignore nulls as main_category_id
        , any_value(case when hierarchy_level = 1 then ingredient_category_id end) ignore nulls as sub_category1_id
        , any_value(case when hierarchy_level = 2 then ingredient_category_id end) ignore nulls as sub_category2_id
        , any_value(case when hierarchy_level = 3 then ingredient_category_id end) ignore nulls as sub_category3_id
        , any_value(case when hierarchy_level = 4 then ingredient_category_id end) ignore nulls as sub_category4_id
    from category_hierarchy
    group by 1, 2
)


, all_tables_joined as (
    select
        md5(concat_ws(
            '-'
            , ingredient_info.ingredient_id
            , ingredient_info.language_id
        )) as pk_dim_ingredients
        , ingredient_info.*
        , group_name_extraction.main_group_name
        , group_name_extraction.category_group_name
        , group_name_extraction.product_group_name
        , flat_hierarchy.main_category_name
        , flat_hierarchy.sub_category1_name
        , flat_hierarchy.sub_category2_name
        , flat_hierarchy.sub_category3_name
        , flat_hierarchy.sub_category4_name
        , flat_hierarchy.main_category_id
        , flat_hierarchy.sub_category1_id
        , flat_hierarchy.sub_category2_id
        , flat_hierarchy.sub_category3_id
        , flat_hierarchy.sub_category4_id
    
    from ingredient_info

    left join group_name_extraction
        on ingredient_info.ingredient_id = group_name_extraction.ingredient_id
        and ingredient_info.language_id = group_name_extraction.language_id
    
    left join flat_hierarchy
        on ingredient_info.ingredient_id = flat_hierarchy.ingredient_id
        and ingredient_info.language_id = flat_hierarchy.language_id
    
    order by ingredient_info.ingredient_id
)

select * from all_tables_joined
