# Menu Optimiser

The Menu Optimizer (MOP) project automatically generates optimized menus for the different Cheffelo brands. It balances business requirements, ingredient storage constraints, and recipe diversity using mixed-integer linear programming through the PuLP optimization library.

## Overview

MOP solves the complex problem of menu planning by considering:
- **Business constraints**: Taxonomy requirements from menu planners
- **Storage limitations**: Cold and dry storage capacity for ingredient picking
- **Menu diversity**: Maximizing variety across multiple categories
- **Recipe requirements**: Required recipes and availability constraints

## Basic Usage

```python
from menu_optimiser.menu_solver import generate_menu, build_response
from menu_optimiser.common import RequestMenu

# Create menu request mimicking the payload from API
payload = RequestMenu(
    year=2024,
    week=12,
    companies=[{
        "company_id": "<COMPANY_ID>",
        "num_recipes": 12,
        "required_recipes": ["123", "456"],
        "available_recipes": ["123", "456", "789", ...],
        "linked_recipes": ["123": [456], ...]
        "taxonomies": [...],  # Business constraints
    }]
)

# Generate optimized menu
problem, menu_df, error_msg = await generate_menu(payload)

# Build API response
response = build_response(
    problem=problem,
    df=menu_df,
    payload=payload,
    error_mesagges=error_msg,
    )
```

### Input Parameters

#### Recipes
- required_recipes: Recipes that must be included (e.g., marketed recipes)
- available_recipes: Pool of selectable recipes (controlled by PIM)
- linked_recipes: Pool of recipes that are linked (controlled by PIM). Two linked recipes should ideally not be on the menu simultaneously.

#### Business Input
The taxonomies represent the business input given to us by the menu planners, where they can adjust:
- Main ingredients: Target quantities for specific main ingredient ids
- Price categories: Target quantities for specific price category ids
- Cooking times: Target quantities for specific cooking times
- Average rating: Minimum avg. rating for a recipe (*currently ignored by the algorithm*)

#### Output

- problem: The optimization problem instance
- menu_df: Generated menu with recipe details
- error_msg: Error messages (if any)

The `build_response` function formats the output for the Analytics API, combining required recipes with optimized selections.


## Optimization Approach

MOP uses a mixed-integer linear programming model that:

- Selects required recipes for the specified brand and week
- Optimizes remaining selections to maximize diversity while meeting constraints
- Adapts dynamically when initial constraints are infeasible

The business input from the payload as well as the constraints set for ingredients and the weights and categories for diversity is translated into decision variables.

**Decision Variables:**
- `z[r]`: Binary variable representing the recipe ids (1 if recipe `r` is selected)
- `x[r][t]`: Binary variable representing recipe id and taxonomy id combinations (1 if recipe `r` is assigned to taxonomy `t`)
- `ingredient_vars[i]`: Binary variable representing the ingredient ids (1 if ingredient `i` is used)
- `tax_max[r][t]`: Binary variable representing recipe id and taxonomy id combinations (1 if recipe `r` is assigned to taxonomy `t`). This variable is under strciter constraints than `x[r][t]`, which can be found in the config.py file.
- `diversity_vars[category][value]`: Binary variables representing specific categories for diversity


**Key Constraints:**
1. **Taxonomy Requirements**: Quantities (both min and max) per taxonomy
2. **Storage Limits**: Cold and dry storage capacity constraints
3. **Recipe-Taxonomy Linking**: Recipes can only fulfill applicable taxonomies
4. **Ingredient Dependencies**: Recipe selection implies ingredient usage
5. **Business Rules**: Price categories, cooking times, main ingredients


PuLP then solves the problem w.r.t. the constraints, as well as trying to optimize the objective function. The objective function is set to optimize variation on the menu.

**Objective Function:**
- Maximize: Σ (diversity_weight × category_weight × diversity_vars[category])

The categories where we want to increase variation are:
- Main protein (e.g. salmon, cod, beef)
- Main carbohydrate (e.g. potto, rice)
- Protein processing (e.g. filet, minced meat)
- Cuisine (e.g. Indian, fusion, nordic)
- Dish type (e.g. burger, pizza)

*Weight and categories are configurable in CompanyOptimizationConfig (config.py)*

The categories and the weights for the objective function can be adjusted in the CompanyOptimizationConfig in config.py, where the max amount of cold storage and dry storage ingredients for the decision variables can be adjusted as well.

### Adaptive Optimization

#### Constraint Relaxation
If the initial business input constraints are infeasible, MOP automatically reduces the taxonomy constraints, to prevent erratic outputs from infeasible problems.

#### Storage adjustment strategy

If the initial storage constraints are infeasible, MOP will:

1. **Attempts**: Gradually trade cold storage for dry storage
2. **Final Resort**: Reduce requested amount of recipes to fit constraints and allowed linked recipes
3. **Maximum Attempts**: 20 iterations with 3-second time limit per solve


### Configuration
Ingredient storage limits and diversity categories + weights can be adjusted in:


```python
# config.py

class OptimizationConfig:         # config for specific company
    max_cold_storage: 126         # maximum amount of ingredients in cold storage
    max_dry_storage: 156          # maximum amount of ingredients in dry storage
    diversity_category_weights: { # categories and weights for diversity objective
        "main_protein": 5.0,
        "main_carb": 4.0,
        "protein_processing": 3.0,
        "cuisine": 2.0,
        "dish_type": 2.0,
    },
    amount_of_recipes: 100        # total wanted (not required) amount of recipes
    taxonomy_maximum: {2015: 9}   # maximum number of recipes (9) per taxonomy id (2015)
```

## Data Sources

The system integrates multiple data sources from Azure Data Lake:

- **Recipe Bank**: Core recipe data provided by PIM, e.g. ratings, cooking times
- **Recipe Tagging**: Recipe ids and their taxonomy classifications provided by the Recipe Tagging project, e.g. dish types, cuisines, etc
- **Carbohydrates**: Recipe ids and their main carbohydrate categories
- **Proteins**: Recipe ids and their main protein categories
- **Ingredients**: Recipe ids and their ingredient ids
