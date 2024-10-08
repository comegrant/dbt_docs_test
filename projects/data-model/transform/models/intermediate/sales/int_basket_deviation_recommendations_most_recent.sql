with 

recommendations as (

    select * from {{ ref('int_basket_deviation_recommendations') }}

)

, recommendations_most_recent as (
    
    select * 
    from recommendations
    where recommendation_recency_group = 1
    
)

select * from recommendations_most_recent