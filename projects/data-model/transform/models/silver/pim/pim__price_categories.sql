with 

source as (

    select * from {{ source('pim', 'pim__price_category') }}

),

renamed as (

    select
        {# ids #}
        company_id
        , portion_id
        , level as price_category_level_id
        
        {# ints #}
        , min_price as price_category_min_total_ingredient_cost
        , max_price as price_category_max_total_ingredient_cost
        , suggested_selling_price_incl_vat as price_category_price_inc_vat

        {# strings #}
        , name as price_category_level_name

        {# dates #}
        , to_date(period_from, 'MMddyy') as price_category_valid_from
        , to_date(period_to, 'MMddyy') as price_category_valid_to

        {# system #}
        , created_by as source_created_by
        , created_at as source_created_at
        , updated_by as source_updated_by
        , updated_at as source_updated_at

        {# not sure if this is needed
        , color
        , text_color
        #}

    from source

)

select * from renamed