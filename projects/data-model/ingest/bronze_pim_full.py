# Databricks notebook source
from coredb_connector import load_coredb_full

# COMMAND ----------

database = "PIM"
tables = ["menus", 
          "menu_recipes",
          "menu_variations",
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
          "recipe_step_sections",
          "recipes_taxonomies",
          "portions",
          "taxonomies",
          "taxonomies_translations",
          "weekly_menus",
          "ingredients",
          "ingredient_categories",
          "order_ingredients",
          "ingredient_categories_translations",
          "chef_ingredients",
          "chef_ingredient_sections",
          "generic_ingredients_translations"
          ]
for table in tables: 
    load_coredb_full(dbutils, database, table)
