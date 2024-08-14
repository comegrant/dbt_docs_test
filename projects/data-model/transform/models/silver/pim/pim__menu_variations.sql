with 

source as (

    select * from {{ source('pim', 'pim__menu_variations') }}

),

renamed as (

    select
        {# ids #}
        menu_variation_id
        , menu_variation_margin_id
        , menu_id
        , portion_id
        , menu_variation_ext_id as product_variation_id

        {# ints #}
        , menu_number_days
        , menu_price
        , menu_cost
        
        {# system #}
        , created_by as system_created_by
        , created_date as system_created_date
        , modified_by as system_modified_by
        , modified_date as system_modified_date

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