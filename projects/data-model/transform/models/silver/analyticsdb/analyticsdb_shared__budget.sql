with 

source as (

    select * from {{ source('analyticsdb', 'analyticsdb_shared__budget') }}

)

, renamed as (

    select

        
        {# ids #}
        id as budget_id
        , budget_type_id
        , budget_parameter_split_id as budget_segment_id
        , company_id

        {# numerics #}
        , budget_year
        , year as financial_year
        , quarter as financial_quarter
        , month as financial_month_number
        , week as financial_week
        , orders
        , atv_gross
        
        {# system #}
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

        

    from source

)

select * from renamed
