with 

recipe_favorites as (

    select * from {{ ref('pim__recipe_favorites') }}

)

, billing_agreements as (

    select * from {{ ref('dim_billing_agreements') }}

)

, companies as (

        select * from {{ ref('dim_companies') }}

)

, add_keys as (

    select
        
        md5(recipe_favorites.recipe_favorite_id) as pk_fact_recipe_reactions
        , recipe_favorites.recipe_favorite_id as recipe_reaction_id
        , recipe_favorites.billing_agreement_id
        , recipe_favorites.recipe_id
        , recipe_favorites.main_recipe_id
        , recipe_favorites.recipe_favorite_type_id as recipe_reaction_type_id
        , recipe_favorites.is_active_reaction
        , recipe_favorites.source_created_at
        , md5(cast(concat(recipe_favorites.recipe_id, companies.language_id) as string)) as fk_dim_recipes
        , md5(companies.company_id) AS fk_dim_companies
        , billing_agreements.pk_dim_billing_agreements as fk_dim_billing_agreements
        , cast(date_format(recipe_favorites.source_created_at, 'yyyyMMdd') as int) as fk_dim_dates
        , md5(cast(recipe_favorites.recipe_favorite_type_id as string)) as fk_dim_recipe_reaction_types
        , cast(date_format(recipe_favorites.source_created_at, 'HHmm') as int) as fk_dim_time
        
    from recipe_favorites
    left join billing_agreements 
        on recipe_favorites.billing_agreement_id = billing_agreements.billing_agreement_id
        and recipe_favorites.source_created_at >= billing_agreements.valid_from
        and recipe_favorites.source_created_at < billing_agreements.valid_to
    left join companies 
        on billing_agreements.company_id = companies.company_id
    
)

select * from add_keys