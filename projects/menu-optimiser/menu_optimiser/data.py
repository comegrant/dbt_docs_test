"""
Data loading and preprocessing for the menu optimiser.
"""

import pandas as pd
from menu_optimiser.common import Args
from menu_optimiser.db import (
    get_recipe_bank_data,
    get_recipe_tagging_data,
    get_recipe_carbs_data,
    get_recipe_protein_data,
)
from menu_optimiser.preprocessing import (
    preprocess_recipe_bank_data,
    preprocess_recipe_tagging,
    preprocess_carbs,
    preprocess_protein,
    preprocess_final_dataset,
)


async def create_dataset(args: Args) -> pd.DataFrame:
    """
    Load and preprocess all required data sources to produce the complete dataset for menu optimisation.

    Args:
        args (Args): Configuration arguments specifying company and environment.

    Returns:
        pd.DataFrame: The dataset ready for the menu solver.
    """
    df_recipe_bank = await get_recipe_bank_data(args.company_code, args.recipe_bank_env)
    df_recipe_tagging = await get_recipe_tagging_data()
    df_carbs = await get_recipe_carbs_data()
    df_protein = await get_recipe_protein_data()

    df_recipe_bank = preprocess_recipe_bank_data(df_recipe_bank)
    df_recipe_tagging = preprocess_recipe_tagging(df_recipe_tagging)
    df_carbs = preprocess_carbs(df_carbs)
    df_protein = preprocess_protein(df_protein)

    df_full = df_recipe_bank.merge(df_recipe_tagging, on="recipe_id", how="left")
    df_full = df_full.merge(df_carbs, on="recipe_id", how="left")
    df_full = df_full.merge(df_protein, on="recipe_id", how="left")

    df_full = preprocess_final_dataset(df_full)

    return df_full
