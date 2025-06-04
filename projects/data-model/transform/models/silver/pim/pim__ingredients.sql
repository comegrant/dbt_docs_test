with 

source as (

    select * from {{ source('pim', 'pim__ingredients') }}

),

renamed as (

    select

        {# ids #}
        ingredient_id
        , supplier_id as ingredient_supplier_id
        , unit_label_id
        , ingredient_category_id
        , ingredient_status_codes_id as ingredient_status_code_id
        , ingredient_type as ingredient_type_id
        , pack_type_id
        , epd as epd_id_number
        , manufacturer_id as ingredient_manufacturer_supplier_id
        
        {# strings #}
        , upper(ingredient_internal_reference) as ingredient_internal_reference
        , ingredient_brand
        , ingredient as ingredient_content_list
        , case when ean like '^[0-9]{13}$' or ean like '^[0-9]{8}$' then ean else null end as ean_code_consumer_packaging
        , case when ean_dpack like '^[0-9]{13}$' or ean_dpack like '^[0-9]{8}$' then ean_dpack else null end as ean_code_distribution_packaging
        , ingredient_external_reference

        {# system #}
        , created_by as source_created_by
        , created_date as source_created_at
        , modified_by as source_updated_by
        , modified_date as source_updated_at

        {# numerics #}
        , cast(ingredient_quantity as int) as ingredient_size
        , shelf_life as ingredient_shelf_life
        , netto_weight as ingredient_net_weight
        , brutto_weight as ingredient_gross_weight
        , supplier_pack_size as ingredient_distribution_packaging_size

        {# ints #}
        , dfp_pallet as distribution_packages_per_pallet
        , depth as ingredient_packaging_depth
        , height as ingredient_packaging_height
        , width as ingredient_packaging_width

        {# booleans #}
        , is_active
        , can_be_used as is_available_for_use
        , is_outgoing
        , cold_storage as is_cold_storage
        , consumer_cold_storage as is_consumer_cold_storage
        , is_organic
        , fragile_ingredient as is_fragile_ingredient
        , special_packing as is_special_packing
        , isnotnull(ingredient_photo) as has_customer_photo
        , isnotnull(ingredient_hover_photo) as has_packaging_photo
        , isnotnull(ingredient_internal_photo) as  has_internal_photo
        , register_batch_number as is_to_register_batch_number
        , register_expiration_date as is_to_register_expiration_date
        , register_temperature as is_to_register_temperature
        , register_ingredient_weight as is_to_register_ingredient_weight

        {# Not Used
        , ingredient_customer_category_id --Customer categories are legacy
        , currency_id --Apparently not used - is 1 for all rows
        , reference_id --All rows are null
        , ingredient_cost_unit --Ingredient_costs are in the pim__ingredient_costs table
        , sales_price --Ingredient_costs are in the pim__ingredient_costs table
        , marketing_price --Ingredient_costs are in the pim__ingredient_costs table
        , packing_priority --Apparently not used, is either null or 0
        , inventory_amount --This should probably be fetched from an inventory table instead
        , unit_customer_pack -- Was not available in mb.get_all_ingredients. Is either 0 or 1, except one row is 5.
        , price_start_date --Seems like legacy. Range from 2015-11-10 to 2019-02-05.
        , price_end_date --Seems like legacy. Is either 2018-09-20, 2018-12-23 or null.
        , minimum_durable_date --All rows are null
        , ingredient_cost_measure --Not sure about this
        , ingredient_tips --Information seems very limited here, not likely to be used
        , cooking_instructions --Information seems very limited here, not likely to be used
        #}

    from source

)

select * from renamed