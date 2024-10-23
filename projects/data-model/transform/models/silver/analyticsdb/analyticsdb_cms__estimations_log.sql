with 

source as (

    select * from {{ source('analyticsdb', 'analyticsdb_cms__estimations_log') }}

)

, renamed as (

    select

        
        {# ids #}
        id as estimations_log_id
        , company_id
        , variation_id as product_variation_id
        , deviation_origin as billing_agreement_basket_deviation_origin_id

        {# numerics #}
        , year as menu_year
        , week as menu_week
        , total_quantity as product_variation_quantity
        
        {# timestamp #}
        , timestamp as estimation_generated_at

    from source

)

select * from renamed
