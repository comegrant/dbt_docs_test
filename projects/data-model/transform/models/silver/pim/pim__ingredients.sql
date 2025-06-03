with 

source as (

    select * from {{ source('pim', 'pim__ingredients') }}

),

renamed as (

    select

        {# ids #}
        ingredient_id
        , supplier_id as ingredient_supplier_id
        --, pack_type_id
        --, unit_label_id
        , ingredient_category_id
        --, currency_id
        --, reference_id
        --, manufacturer_id
        , ingredient_status_codes_id as ingredient_status_code_id
        --, ingredient_customer_category_id
        
        {# strings #}
        , upper(ingredient_internal_reference) as ingredient_internal_reference
        --, ingredient_external_reference
        --, ingredient_brand
        --, ingredient_photo
        --, ingredient_hover_photo
        --, cooking_instructions
        --, ingredient_tips --mainly nan
        , ingredient as ingredient_content-- list of contained ingredients
        , ingredient_type -- hash id
        --, ingredient_internal_photo --mainly nan
        --, ean -- some errors in data
        --, ean_dpack -- looks like int

        {# system #}
        , created_by as source_created_by
        , created_date as source_created_at
        , modified_by as source_updated_by
        , modified_date as source_updated_at

        --{# numerics #}
        --, ingredient_quantity
        --, ingredient_cost_measure
        --, ingredient_cost_unit
        --, supplier_pack_size
        --, shelf_life
        , netto_weight
        --, brutto_weight
        --, sales_price
        --, marketing_price --mainly nan
        --, epd -- looks like an int

        --{# ints #}
        --, packing_priority --mainly nan
        --, inventory_amount
        --, unit_customer_pack -- 0 or 1 (ine row is '5')
        --, dfp_pallet
        --, depth
        --, height
        --, width

        {# booleans #}
        --, cold_storage as is_cold_storage
        , is_active
        --, can_be_used
        --, consumer_cold_storage as is_consumer_cold_storage
        --, special_packing as is_special_packing
        --, is_outgoing
        --, register_temperature
        --, register_expiration_date
        --, register_batch_number
        --, register_ingredient_weight
        --, fragile_ingredient as is_fragile_ingredient
        --, is_organic

        --{# dates #}
        --, price_start_date 
        --, price_end_date

        --{# timestamps #}
        --, minimum_durable_date --only nan

    from source

)

select * from renamed