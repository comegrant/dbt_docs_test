# Databricks notebook source
import sys

sys.path.append("../helper_functions")

from coredb_connector import load_coredb_full

# COMMAND ----------

database = "PIM"
tables = [
    "allergies_preference",
    "allergies_translations",
    "chef_ingredients",
    "chef_ingredient_sections",
    "generic_ingredients_translations",
    "ingredients",
    "ingredient_allergies",
    "ingredient_categories",
    "ingredient_categories_translations",
    "ingredient_category_preference",
    "ingredient_nutrient_facts",
    "ingredients_translations",
    "menus",
    "menu_recipes",
    "menu_variations",
    "order_ingredients",
    "portions",
    "portions_translations",
    "order_ingredients",
    "portions",
    "recipes",
    "recipes_comments",
    "recipe_companies",
    "recipe_difficulty_levels_translations",
    "recipe_main_ingredients",
    "recipe_main_ingredients_translations",
    "recipes_metadata",
    "recipe_metadata_translations",
    "recipe_portions",
    "recipes_rating",
    "recipe_steps",
    "recipe_steps_translations",
    "recipe_step_sections",
    "recipes_translations",
    "status_codes_translations",
    "taxonomies_translations",
    "taxonomy_types",
    "weekly_menus",
    "recipe_favorite_types",
]
for table in tables:
    load_coredb_full(dbutils, database, table)
