with 

source as (

    select * from {{ source('pim', 'pim__menu_variations') }}

),

renamed as (

    select
        {# ids #}
        menu_variation_id
        -- Not sure what this is
        --, menu_variation_margin_id
        , menu_id
        , portion_id
        , menu_variation_ext_id as product_variation_id

        {# ints #}
        , menu_number_days
        -- Should maybe be generated from ingredients
        --, menu_price
        --, menu_cost
        
        {# system #}
        , created_by as source_created_by
        , created_date as source_created_at
        , modified_by as source_updated_by
        , modified_date as source_updated_at

        {# no clue what this is:
        , uploaded_pdf
        , uploaded_pdf_eng
        , target_margin
        , packaging_time
        , price
        , manual_estimates
        , campaign_price
        , produce_menu_leaflet
        #}

    from source

)

select * from renamed