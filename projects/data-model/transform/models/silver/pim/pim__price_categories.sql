with 

source as (

    select * from {{ ref('scd_pim__price_categories') }}

),

renamed as (

    select
        {# ids #}
        company_id
        , portion_id
        , level as price_category_level_id
        
        {# ints #}
        , coalesce(min_price,0) as min_ingredient_cost_inc_vat
        , coalesce(max_price,9999) as max_ingredient_cost_inc_vat
        , suggested_selling_price_incl_vat as suggested_price_inc_vat

        {# strings #}
        , name as price_category_level_name

        {# dates #}
        , dbt_valid_from as valid_from
        , dbt_valid_to  as valid_to

        {# system #}
        , created_by as source_created_by
        , created_at as source_created_at
        , updated_by as source_updated_by
        , updated_at as source_updated_at

    from source

)

select * from renamed
