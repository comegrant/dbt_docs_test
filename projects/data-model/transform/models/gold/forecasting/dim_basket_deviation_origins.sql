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
    from billing_agreement_basket_deviation_origins
    
    union 
    
    select
        md5('00000000-0000-0000-0000-000000000000') as pk_dim_basket_deviation_origins
        , '00000000-0000-0000-0000-000000000000' as billing_agreement_basket_deviation_origin_id
        , 'No deviation' as basket_deviation_origin_name
        
)

select * from add_no_deviation_row

