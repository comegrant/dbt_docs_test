with 

source as (

   select * from {{ source('analyticsdb', 'analyticsdb_analytics__billing_agreement_basket_product_log') }}

)

, renamed (

   select 

      agreement_id as billing_agreement_id
      , current_delivery_year as menu_year
      , current_delivery_week as menu_week
      , billing_agreement_basket_id
      , subscribed_product_variation_id as product_variation_id
      , delivery_week_type as product_delivery_week_type_id
      , quantity as product_variation_quantity
      , is_extra
      , logging_date as source_created_at
      , "AnalyticsDB Log" as source_created_by
   from source

)
   

select * from renamed