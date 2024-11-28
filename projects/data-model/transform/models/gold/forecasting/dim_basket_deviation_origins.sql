with 

billing_agreement_basket_deviation_origins as (

    select 
        *
    from {{ ref('cms__billing_agreement_basket_deviation_origins') }}

)

, add_no_deviation_row as (

    select         
        md5(billing_agreement_basket_deviation_origin_id) as pk_dim_basket_deviation_origins
        , billing_agreement_basket_deviation_origin_id
        , basket_deviation_origin_name
        , basket_deviation_origin_source_name
        , preselector_category
    from billing_agreement_basket_deviation_origins
    
    union 
    
    select
        md5('00000000-0000-0000-0000-000000000000') as pk_dim_basket_deviation_origins
        , '00000000-0000-0000-0000-000000000000' as billing_agreement_basket_deviation_origin_id
        , 'No Deviation' as basket_deviation_origin_name
        , 'No Deviation' as basket_deviation_origin_source_name
        , 'Not Preselector' as preselector_category
)

select * from add_no_deviation_row

