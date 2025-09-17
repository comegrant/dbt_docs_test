with 

recipe_rating_quick_comments as (  
    select * from  {{ ref('pim__recipe_rating_quick_comments') }}  
)  

, add_keys as (  

    select 
        md5(concat(  
            recipe_rating_id::string  
            , quick_comment_id::string  
        )) as pk_bridge_recipe_reviews_quick_comments  
        , md5(recipe_rating_id::string) as fk_dim_recipe_reviews  
        , md5(quick_comment_id::string) as fk_dim_quick_comments  
        , recipe_rating_id  
        , quick_comment_id  
    from {{ ref('pim__recipe_rating_quick_comments') }}  
    
)  

select * from add_keys  