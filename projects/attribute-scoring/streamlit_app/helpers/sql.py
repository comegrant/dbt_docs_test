attribute_scoring_query = """
    select
        *
    from mloutputs.attribute_scoring
    where is_latest=True
    """


weekly_menu_query = """
    select distinct
        fm.company_id,
        fm.recipe_id,
        fm.menu_year,
        fm.menu_week,
        dr.recipe_name,
        dr.recipe_difficulty_name,
        dr.recipe_main_ingredient_name_local,
        dr.cooking_time_to,
        dr.cooking_time_from,
        recipe_photo
    from gold.fact_menus fm
    left join gold.dim_recipes dr
        on dr.pk_dim_recipes = fm.fk_dim_recipes
    left join silver.pim__recipe_metadata rm
        on dr.recipe_metadata_id = rm.recipe_metadata_id
    where fm.menu_year >= 2024
    and fm.recipe_id is not null
    """

feedback_query = """
    select
        recipe_id
    from mltesting.attribute_scoring_feedback
"""
