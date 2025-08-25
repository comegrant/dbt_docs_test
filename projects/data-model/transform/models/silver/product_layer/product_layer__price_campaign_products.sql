with

source as (

    select * from {{ source('product_layer', 'product_layer__price_campaign_product') }}

)

, renamed_and_type_casted as (

    select
    {# ids #}
        id                                   as price_campaign_product_id
        , company_id
        , product_variation_id

        {# ints #}
        , menu_week_start                    as menu_week_from
        , menu_year_start                    as menu_year_from
        , menu_week_end                      as menu_week_to
        , menu_year_end                      as menu_year_to

        {# numerics #}
        , price_without_vat                  as price_ex_vat
        , (cast(vat as decimal(6, 4)) / 100) as vat
        , target_margin

        {# timetsamps #}
        , start_date_utc                     as valid_from
        , end_date_utc                       as valid_to

        {# booleans #}
        , is_base_campaign

        {# strings #}
        , comment

        {# system #}
        , created_at_utc                     as source_created_at
        , created_by                         as source_created_by
        , updated_at_utc                     as source_updated_at
        , updated_by                         as source_updated_by

    from source

)

select * from renamed_and_type_casted
