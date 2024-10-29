# :construction: Attribute Scoring :construction:

## Project Overview

This project aims to classify recipes based on certain fuzzy attributes. A recipe can have a binary attribute, i.e. vegetarian, where the recipe is or is not vegetarian. A recipe can also have a fuzzy attribute, i.e. family friendly, where the degree to which the recipe fits the attribute can vary. We want to assess **"how much"** a recipe exhibits a certain fuzzy attribute. For example, a recipe could be **family-friendly**, but this quality isn't binary. Instead, we assign a value between `0` and `1`, where:

- `0`: Not family-friendly at all
- `0.5`: Somewhat family-friendly
- `1`: Very family-friendly

This project is a **work-in-progress** and serves as a baseline for future improvements.

## Features of the Project

- **Data**:
    - Dbt model: `env.mlgold.ml_recipes`
    - Feature table: `env.mlfeatures.ft_ml_recipes`
    - Model output: `env.mloutputs.attribute_scoring`
  
- **Model Training & Prediction**:
  - Trains a classifier model for a specific company and a selected "fuzzy" attribute as the target. The target can be `family_friendly` or `family_friendly`
  - Predicts the likelihood (probability) that a recipe possesses the fuzzy attribute.
  - The output also includes a configurable threshold to determine whether the recipe gets a true/false classification for the target.

## Pipelines

The project includes three pipelines:

1. **Tune Pipeline**:
   - Runs Optuna hyperparameter tuning on the model.
   - Triggered manually to refine and improve model performance.
   - Databricks Job parameters:
        - company (LMK, AMK, GL, RT): Cheffelo company abbreviation
        - target (has_chefs_favorite_taxonomy, has_family_friendly_taxonomy): Recipe attribute
        - n_trials (int): Number of trials for Optuna hyperparameter tuning

2. **Train Pipeline**:
   - Trains the model for a given company and target.
   - Triggered manually.
   - Databricks Job parameters:
        -  company (LMK, AMK, GL, RT): Cheffelo company abbreviation
        - target (has_chefs_favorite_taxonomy, has_family_friendly_taxonomy): Recipe attribute

3. **Predict Pipeline**:
   - Automatically triggered each week on Monday at 08:00 AM to run predictions for the latest batch of recipes.
   - Databricks Job parameter:
        - run_mode (weekly, bulk): Run normal prediction (weekly), or predict all recipes 


## How to Use

### 2. Configure the Pipelines
The pipelines can be triggered manually in Databricks or automatically.
- The target, environment and company can be configured directly in Databricks before triggering job for tuning and training.
- Threshold for the true/false classification can be configured in `predict/config.py`.


## Streamlit app

TBD.

## Usage

You can run the project in two different ways.

- CLI

### CLI
To run the project from the CLI, use the following command from the root dir.

```bash
python -m attribute_scoring.main --name "Agathe Raaum"
```

```bash
chef up
```
