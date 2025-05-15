with 

source as (

    select * from {{ source('analyticsdb', 'analyticsdb_shared__budget_marketing_input') }}

)

, renamed as (

    select

        {# ids #}
        id as budget_marketing_input_id
        , company_id
        , budget_type_id
        
        {# numerics #}
        , estimate as monthly_aquisition_target
        , year as financial_year
        , month as financial_month_number
        , budget_year
        
        {# system #}
        , created_by as source_created_by
        , created_at as source_created_at
        , updated_by as source_updated_by
        , updated_at as source_updated_at

    from source

)

select * from renamed
