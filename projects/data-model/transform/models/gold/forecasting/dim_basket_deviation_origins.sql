with 

billing_agreement_basket_deviation_origins as (
    select 
        md5(billing_agreement_basket_deviation_origin_id) as pk_dim_basket_deviation_origins
        , basket_deviation_origin_name
    from {{ ref('cms__billing_agreement_basket_deviation_origins') }}
)

, no_deviation_origin_id as (
    select 
        md5('00000000-0000-0000-0000-000000000000') as pk_dim_basket_deviation_origins
        , 'No deviation' as basket_deviation_origin_name
)

, all_tables_unioned as (
    select * from billing_agreement_basket_deviation_origins
    union 
    select * from no_deviation_origin_id
)

select * from all_tables_unioned

