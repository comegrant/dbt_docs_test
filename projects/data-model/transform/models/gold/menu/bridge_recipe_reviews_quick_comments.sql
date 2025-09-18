with 

reviews as (  
    select * from  {{ ref('dim_recipe_reviews') }}  
)  

, exploded as (
    select 
        pk_dim_recipe_reviews as fk_dim_recipe_reviews
        , trim(exploded.col) as quick_comment_id
    from reviews
    
    cross join lateral explode(split(quick_comment_id_combination, ', ')) as exploded

    where quick_comment_id_combination is not null
)

, add_keys as (  

    select 
        md5(concat(  
            fk_dim_recipe_reviews::string  
            , quick_comment_id::string  
        )) as pk_bridge_recipe_reviews_quick_comments 
        , quick_comment_id
        , fk_dim_recipe_reviews
        , md5(quick_comment_id::string) as fk_dim_recipe_quick_comments
    from exploded
    
)  

select * from add_keys  