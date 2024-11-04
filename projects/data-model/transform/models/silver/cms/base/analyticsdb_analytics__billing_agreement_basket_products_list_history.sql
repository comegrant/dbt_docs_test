with 

source as (

   select * from {{ ref('analyticsdb_analytics__billing_agreement_basket_products_log') }}

)

, basket_product_list (

   select 
      billing_agreement_basket_id
      , array_sort(
         collect_list(
         struct(
            product_variation_id 
            , product_variation_quantity
            , is_extra)
         )
      ) as basket_products_list
      , source_created_at
      , source_created_by
   from source
   group by 1,3,4

)

, basket_product_list_scd2 (

   select
      billing_agreement_basket_id
      , basket_products_list
      , source_created_at as valid_from
      , {{ get_scd_valid_to('source_created_at', 'billing_agreement_basket_id') }} as valid_to
      , source_created_at
      , source_created_by
   from basket_product_list

)

select * from basket_product_list_scd2