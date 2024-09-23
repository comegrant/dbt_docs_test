with

adams_postgres as (

    select * from {{ref('base_postgres_net_backend_adams__ce_deviation_ordered')}}

),

linas_postgres as (

    select * from {{ref('base_postgres_net_backend_linas__ce_deviation_ordered')}}
    
),

retnemt_postgres as (

    select * from {{ref('base_postgres_net_backend_retnemt__ce_deviation_ordered')}}
    
),

godtlevert_postgres as (

    select * from {{ref('base_postgres_net_backend_godtlevert__ce_deviation_ordered')}}
    
),

linas_segment as (

    select * from {{ref('base_segment_net_backend_linas__ce_deviation_ordered')}}
    
),

adams_segment as (

    select * from {{ref('base_segment_net_backend_adams__ce_deviation_ordered')}}
    
),

godtlevert_segment as (

    select * from {{ref('base_segment_net_backend_godtlevert__ce_deviation_ordered')}}
    
),

retnemt_segment as (

    select * from {{ref('base_segment_net_backend_retnemt__ce_deviation_ordered')}}
    
),

linas_history as (
    select 
        linas_postgres.*
    from linas_postgres
    left join linas_segment
    on linas_postgres.id = linas_segment.id
    where linas_segment.id is null
),

adams_history as (
    select 
        adams_postgres.*
    from adams_postgres
    left join adams_segment
    on adams_postgres.id = adams_segment.id
    where adams_segment.id is null
),

godtlevert_history as (
    select 
        godtlevert_postgres.*
    from godtlevert_postgres
    left join godtlevert_segment
    on godtlevert_postgres.id = godtlevert_segment.id
    where godtlevert_segment.id is null
),

retnemt_history as (
    select 
        retnemt_postgres.*
    from retnemt_postgres
    left join retnemt_segment
    on retnemt_postgres.id = retnemt_segment.id
    where retnemt_segment.id is null
),

linas_unioned as (
    select * from linas_history
    union all
    select * from linas_segment
),

adams_unioned as (
    select * from adams_history
    union all
    select * from adams_segment
),

godtlevert_unioned as (
    select * from godtlevert_history
    union all
    select * from godtlevert_segment
),

retnemt_unioned as (
    select * from retnemt_history
    union all
    select * from retnemt_segment
),

all_companies_unioned as (
    select 'linas' as company, * from linas_unioned
    union all
    select 'adams' as company, * from adams_unioned
    union all
    select 'godtlevert' as company, * from godtlevert_unioned
    union all
    select 'retnemt' as company, * from retnemt_unioned
),

products_exploded as (
  SELECT 
    *
    , explode(
        from_json(
            products, 'array<struct<productId:string,productName:string,productPrice:int,productQuantity:int,productTypeId:string,productTypeName:string,productVariationId:string,productVariationName:string>>')
        ) as products_array
  FROM all_companies_unioned
),

renamed as (

    select
        --ids
        md5(concat(id, company, products_array.productVariationId)) as deviations_ordered_id
        , id as ce_deviation_ordered_id
        , action_done_by
        , agreement_id as billing_agreement_id

        ,
        case
            when company = 'linas'      then '6A2D0B60-84D6-4830-9945-58D518D27AC2'
            when company = 'adams'      then '8A613C15-35E4-471F-91CC-972F933331D7'
            when company = 'godtlevert' then '09ECD4F0-AE58-4539-8E8F-9275B1859A19'
            when company = 'retnemt'    then '5E65A955-7B1A-446C-B24F-CFE576BF52D7'
            else null
        end as company_id

        --numerics
        , year as menu_year
        , week as menu_week

        --dates
        , {{ get_iso_week_start_date('year', 'week') }} as menu_week_monday_date

        --strings
        , event_text
        , products_array.productId as product_id
        , products_array.productName as product_name
        , products_array.productPrice as product_price
        , products_array.productQuantity as product_variation_quantity
        , products_array.productTypeId as product_type_id
        , products_array.productTypeName as product_type_name
        , products_array.productVariationId as product_variation_id
        , products_array.productVariationName as product_variation_name
        
        --objects
        , taste_preferences
        , concept_preferences
        
        --timestamps
        , received_at as source_received_at
        , sent_at as source_sent_at
        

    from products_exploded

)

select * from renamed