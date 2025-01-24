with 

source as (

   select * from {{ source('analyticsdb', 'analyticsdb_orders__historical_order_lines_combined') }}

)

, renamed as (

   select 

      {# ids #}
      id as billing_agreement_order_line_id
      , order_id as billing_agreement_order_id
      , variation_id as product_variation_id
     
      {# strings #}
      , upper(trim(typeOfLine)) as order_line_type_name
 
      {# numerics #}
      , price as unit_price_ex_vat
      , (CAST(VAT as decimal (6, 4)) / 100) as vat
      , variation_qty as product_variation_quantity


   from source

)

select * from renamed