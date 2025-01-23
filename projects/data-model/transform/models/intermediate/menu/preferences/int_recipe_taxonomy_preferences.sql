with

recipe_taxonomies as (

    select * from {{ ref('pim__recipe_taxonomies') }}

)

, taxonomy_preference_mapping as (
    select
        recipe_taxonomies.recipe_id
        , case
            when
                recipe_taxonomies.taxonomy_id in (
                    {{ var('onesub_chefs_favorite_taxonomy_ids') | join(', ') }}
                ) then 'C94BCC7E-C023-40CE-81E0-C34DA3D79545' --OneSub: Chefs Favorite
            when
                recipe_taxonomies.taxonomy_id in (
                    {{ var('onesub_vegetarian_taxonomy_ids') | join(', ') }}
                ) then '6A494593-2931-4269-80EE-470D38F04796' --OneSub: Vegetarian
            when
                recipe_taxonomies.taxonomy_id in (
                    {{ var('onesube_low_calorie_taxonomy_ids') | join(', ') }}
                ) then 'FD661CAD-7F45-4D02-A36E-12720D5C16CA' --OneSub: Low Calorie
            when
                recipe_taxonomies.taxonomy_id in (
                    {{ var('onesub_quick_and_easy_taxonomy_ids') | join(', ') }}
                ) then 'C28F210B-427E-45FA-9150-D6344CAE669B' --OneSub: Quick and Easy
            when
                recipe_taxonomies.taxonomy_id in (
                    {{ var('onesub_family_friendly_taxonomy_ids') | join(', ') }}
                ) then 'B172864F-D58E-4395-B182-26C6A1F1C746' --OneSub: Family Friendly
            else null
        end as preference_id
    from recipe_taxonomies
)

, recipe_taxonomy_preference as (
    select distinct *
    from taxonomy_preference_mapping
    where preference_id is not null
)

select * from recipe_taxonomy_preference
