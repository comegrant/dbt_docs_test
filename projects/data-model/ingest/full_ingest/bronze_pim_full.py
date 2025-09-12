# Databricks notebook source
import sys
sys.path.append('../../reusable')

from coredb_connector import load_coredb_full

# COMMAND ----------

database = "PIM"
tables = [
    "allergies_preference",
    "allergies_translations",
    "chef_ingredients",
    "chef_ingredient_sections",
    "generic_ingredients",
    "generic_ingredients_translations",
    "ingredients",
    "ingredient_allergies",
    "ingredient_categories",
    "ingredient_categories_translations",
    "ingredient_category_preference",
    "ingredient_nutrient_facts",
    "ingredient_price",
    "ingredient_type"
    "ingredients_status_codes",
    "ingredients_translations",
    "menus",
    "menu_recipes",
    "menu_variations",
    "nutrient_facts_translations",
    "order_ingredients",
    "portions",
    "portions_translations",
    "order_ingredients",
    "portions",
    "price_category",
    "procurement_cycle",
    "quick_comments",
    "quick_comment_translations",
    "rating_quick_comment_configuration",
    "recipes",
    "recipes_comments",
    "recipe_companies",
    "recipe_difficulty_levels_translations",
    "recipe_favorite_types",
    "recipe_main_ingredients",
    "recipe_main_ingredients_translations",
    "recipes_metadata",
    "recipe_metadata_translations",
    "recipe_portions",
    "recipes_rating",
    "recipe_rating_quick_comments",
    "recipe_steps",
    "recipe_steps_translations",
    "recipe_step_sections",
    "recipes_translations",
    "status_codes_translations",
    "suppliers",
    "taxonomies_translations",
    "taxonomy_types",
    "unit_labels",
    "unit_labels_translations",
    "weekly_ingredients",
    "weekly_menus",
    "weekly_orders",
    "weekly_orders_lines",
    "weekly_production_need"
]
for table in tables:
    load_coredb_full(dbutils, database, table)
