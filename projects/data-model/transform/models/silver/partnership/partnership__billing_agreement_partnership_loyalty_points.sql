with 

source as (

    select * from {{ source('partnership', 'partnership__billing_agreement_partnership_loyalty_points') }}

)

, renamed as (

    select

        {# ids #}
        id as billing_agreement_partnership_loyalty_point_id 
        , parent_id as billing_agreement_partnership_loyalty_points_parent_id
        , event_id as billing_agreement_partnership_loyalty_points_event_id
        , billing_agreement_order_id
        , company_partnership_id
        , partnership_rule_id

        {# ints #}
        , agreement_id as billing_agreement_id
        , transaction_points as transaction_points
        
        {# system #}  
        , created_at as source_created_at
        , created_by as source_created_by
        , updated_at as source_updated_at
        , updated_by as source_updated_by

    from source

)

select * from renamed
