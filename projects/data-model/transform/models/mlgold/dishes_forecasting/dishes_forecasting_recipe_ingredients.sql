with ingredient_features as (
    select
        pk_dim_ingredients
        , ingredient_id
        , language_id
        , cast(ingredient_category_id in (1505, 627) as int)                                            as has_chicken_filet
        , cast(ingredient_category_id in (1503, 1505, 627) as int)                                      as has_chicken
        , cast(ingredient_category_id in (2191, 2192, 2193, 2194, 2195, 2197, 2199, 2310, 2311) as int) as has_dry_pasta
        , cast(ingredient_category_id in (2183) as int)                                                 as has_fresh_pasta
        , cast(ingredient_category_id in (1238, 1295, 1238, 2345, 1276) as int)                         as has_white_fish_filet
        , cast(ingredient_category_id in (1238, 1295) as int)                                           as has_cod_fillet
        , cast(ingredient_category_id in (1246) as int)                                                 as has_breaded_cod
        , cast(ingredient_category_id in (1218, 616) as int)                                            as has_salmon_filet
        , cast(ingredient_category_id in (1215, 2231, 1238, 1295, 1238, 2345, 1276, 1218, 616) as int)  as has_seafood
        , cast(ingredient_category_id in (1083, 1070) as int)                                           as has_pork_filet
        , cast(ingredient_category_id in (1085) as int)                                                 as has_pork_cutlet
        , cast(ingredient_category_id in (1333) as int)                                                 as has_trout_filet
        , cast(ingredient_category_id in (2358, 2357) as int)                                           as has_parmasan
        , cast(ingredient_category_id in (1611, 2358, 2357) as int)                                     as has_cheese
        , cast(ingredient_category_id in (1113, 1142, 1084, 1506, 1200) as int)                         as has_minced_meat
        , cast(ingredient_category_id in (1136, 1194, 1147) as int)                                     as has_burger_patty
        , cast(ingredient_category_id in (2205) as int)                                                 as has_noodles
        , cast(ingredient_category_id in (1096, 1079) as int)                                           as has_sausages
        , cast(ingredient_category_id in (1707, 1743) as int)                                           as has_tortilla
        , cast(ingredient_category_id in (1712) as int)                                                 as has_pizza_crust
        , cast(ingredient_category_id in (1081) as int)                                                 as has_bacon
        , cast(ingredient_category_id in (2082) as int)                                                 as has_wok_sauce
        , cast(ingredient_category_id in (2082, 2367, 2375, 2020, 2083, 2019, 2037) as int)             as has_asian_sauces
        , cast(ingredient_category_id in (2025) as int)                                                 as has_salsa
        , cast(ingredient_category_id in (1708) as int)                                                 as has_flat_bread
        , cast(ingredient_category_id in (1729) as int)                                                 as has_pita
        , cast(ingredient_category_id in (980) as int)                                                  as has_whole_salad
        , cast(ingredient_category_id in (944) as int)                                                  as has_shredded_vegetables
        , cast(ingredient_category_id in (968, 968) as int)                                             as has_potato
        , cast(ingredient_category_id in (1014) as int)                                                 as has_peas
        , cast(ingredient_category_id in (1064) as int)                                                 as has_rice
        , cast(ingredient_category_id in (1028) as int)                                                 as has_nuts
        , cast(ingredient_category_id in (990, 989) as int)                                             as has_beans
        , cast(ingredient_category_id in (961) as int)                                                  as has_onion
        , cast(ingredient_category_id in (922) as int)                                                  as has_citrus
        , cast(ingredient_category_id in (1034) as int)                                                 as has_sesame
        , cast(ingredient_category_id in (959) as int)                                                  as has_herbs
        , cast(ingredient_category_id in (926) as int)                                                  as has_fruit
        , cast(ingredient_category_id in (957) as int)                                                  as has_cucumber
        , cast(ingredient_category_id in (965) as int)                                                  as has_chili
        , cast(ingredient_category_id in (1714) as int)                                                 as has_pancake
    from {{ ref('dim_ingredients') }}
)

, bridge_ingredients_combination_joined_features as (
    select
        ingredient_features.*
        , fk_dim_ingredient_combinations
    from
        ingredient_features
    left join {{ ref('bridge_ingredient_combinations_ingredients') }}
        on
            ingredient_features.pk_dim_ingredients
            = {{ ref('bridge_ingredient_combinations_ingredients') }}.fk_dim_ingredients
)

, bridge_ingredients_combination_aggregated as (
    select
        fk_dim_ingredient_combinations
        , language_id
        , cast(sum(has_chicken_filet)>0 as int)       as has_chicken_filet
        , cast(sum(has_chicken)>0 as int)             as has_chicken
        , cast(sum(has_dry_pasta)>0 as int)           as has_dry_pasta
        , cast(sum(has_fresh_pasta)>0 as int)         as has_fresh_pasta
        , cast(sum(has_white_fish_filet)>0 as int)    as has_white_fish_filet
        , cast(sum(has_cod_fillet)>0 as int)          as has_cod_fillet
        , cast(sum(has_breaded_cod)>0 as int)         as has_breaded_cod
        , cast(sum(has_salmon_filet)>0 as int)        as has_salmon_filet
        , cast(sum(has_seafood)>0 as int)             as has_seafood
        , cast(sum(has_pork_filet)>0 as int)          as has_pork_filet
        , cast(sum(has_pork_cutlet)>0 as int)         as has_pork_cutlet
        , cast(sum(has_trout_filet)>0 as int)         as has_trout_filet
        , cast(sum(has_parmasan)>0 as int)            as has_parmasan
        , cast(sum(has_cheese)>0 as int)              as has_cheese
        , cast(sum(has_minced_meat)>0 as int)         as has_minced_meat
        , cast(sum(has_burger_patty)>0 as int)        as has_burger_patty
        , cast(sum(has_noodles)>0 as int)             as has_noodles
        , cast(sum(has_sausages)>0 as int)            as has_sausages
        , cast(sum(has_tortilla)>0 as int)            as has_tortilla
        , cast(sum(has_pizza_crust)>0 as int)         as has_pizza_crust
        , cast(sum(has_bacon)>0 as int)               as has_bacon
        , cast(sum(has_wok_sauce)>0 as int)           as has_wok_sauce
        , cast(sum(has_asian_sauces)>0 as int)        as has_asian_sauces
        , cast(sum(has_salsa)>0 as int)               as has_salsa
        , cast(sum(has_flat_bread)>0 as int)          as has_flat_bread
        , cast(sum(has_pita)>0 as int)                as has_pita
        , cast(sum(has_whole_salad)>0 as int)         as has_whole_salad
        , cast(sum(has_shredded_vegetables)>0 as int) as has_shredded_vegetables
        , cast(sum(has_potato)>0 as int)              as has_potato
        , cast(sum(has_peas)>0 as int)                as has_peas
        , cast(sum(has_rice)>0 as int)                as has_rice
        , cast(sum(has_nuts)>0 as int)                as has_nuts
        , cast(sum(has_beans)>0 as int)               as has_beans
        , cast(sum(has_onion)>0 as int)               as has_onion
        , cast(sum(has_citrus)>0 as int)              as has_citrus
        , cast(sum(has_sesame)>0 as int)              as has_sesame
        , cast(sum(has_herbs)>0 as int)               as has_herbs
        , cast(sum(has_fruit)>0 as int)               as has_fruit
        , cast(sum(has_cucumber)>0 as int)            as has_cucumber
        , cast(sum(has_chili)>0 as int)               as has_chili
        , cast(sum(has_pancake)>0 as int)             as has_pancake
    from bridge_ingredients_combination_joined_features
    group by fk_dim_ingredient_combinations, language_id

)

, fact_menus as (
    select distinct
        recipe_portion_id
        , language_id
        , fk_dim_ingredient_combinations
    from {{ ref('fact_menus') }}
)

, final as (
    select
        fact_menus.recipe_portion_id
        , bridge_ingredients_combination_aggregated.* except (fk_dim_ingredient_combinations)
    from fact_menus left join bridge_ingredients_combination_aggregated
        on
            fact_menus.fk_dim_ingredient_combinations
            = bridge_ingredients_combination_aggregated.fk_dim_ingredient_combinations
            and fact_menus.language_id = bridge_ingredients_combination_aggregated.language_id
    where recipe_portion_id is not null
)

select * from final
order by recipe_portion_id
