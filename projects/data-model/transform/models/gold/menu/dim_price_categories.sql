with 

source as (

    select * from {{ ref('pim__price_categories') }}

)

, add_pk as (

    select
 
        md5(concat_ws('-', company_id, portion_id, price_category_level_id, valid_from)) as pk_dim_price_categories
        , company_id
        , portion_id
        , price_category_level_id
        , min_ingredient_cost_inc_vat
        , max_ingredient_cost_inc_vat
        , suggested_price_inc_vat
        , price_category_level_name
        , valid_from
        , valid_to

    from source

)

, add_unknown_row as (

    select * from add_pk

    union all

    select 
        '0' as pk_dim_price_categories
        , '0' as company_id
        , 0 as portion_id
        , 0 as price_category_level_id
        , 0 as min_ingredient_cost_inc_vat
        , 0 as max_ingredient_cost_inc_vat
        , 0 as suggested_price_inc_vat
        , 'unknown' as suggested_price_inc_vat
        , cast('1970-01-01' as timestamp) as valid_from
        , cast('9999-12-31' as timestamp) as valid_to

)

select * from add_unknown_row