# Changelog
All notable changes to this project will be documented in this file.

.....

## [Unreleased]

## [0.3.20]
MenuOptimization: Changing column name coming from Ingredient Bank, due to PIM change and price bug fix.

## [0.3.19]
MenuOptimization: Adding flag to when constrains are met without fullfiling the recipes and use lower constraints instead of upper.

## [0.3.16]
MenuOptimization: Price Ranges in input

## [0.3.13]
- Preselector: Fixing main recipe id 2d list to 1d

## [0.3.12]
- Preselector: Fixing api preprocessing

## [0.3.11]
- Menu Optimization: Adding project to dev run
- Menu Optimization: Reading from recipe bank env
- Menu Optimization: Some refactoring, moving code into seperate files
## [0.3.10]
- Preselector: Adding quarantined dishes and adding penalized score
## [0.3.9]
- Preselector: Filtering preference rules on product id

## [0.3.8]
- Menu Optimizarion: Input errors feedback for rating scale and ingredients and taxonomies not in recipe bank.

## [0.3.7]
- Preselector: Fix logging, only log to file if env variable is true.
## [0.3.6]
- Preselector: Adding rec engine score to rank dishes.
- Preselector: Using protein variety to avoid repeating proteins in dishes.
- Preselector: Adding preference rules defined by chefs to avoid repeating too many different carbohydrates.

## [0.2.0]
- Adding menu optimization to personalization code repository

## [0.0.1] -
- Initial release of basket preselector, including filtering by preferences and selection of dishes for a specific week
