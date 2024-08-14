# Databricks notebook source
from coredb_connector import load_coredb_full

# COMMAND ----------

database = "PIM"
tables = ["menus", 
          "menu_recipes",
          "menu_variations",
          "recipes",
          "recipe_companies",
          "recipe_difficulty_levels_translations",
          "recipe_main_ingredients",
          "recipe_main_ingredients_translations",
          "recipes_metadata",
          "recipe_metadata_translations",
          "recipe_portions",
          "recipes_taxonomies",
          "portions",
          "taxonomies",
          "taxonomies_translations",
          "weekly_menus"
          ]
for table in tables: 
    load_coredb_full(dbutils, database, table)
