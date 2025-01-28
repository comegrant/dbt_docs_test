import logging

import numpy as np
import pandas as pd
from aligned import ContractStore
from data_contracts.recipe import RecipeFeatures

from preselector.store import preselector_store

logger = logging.getLogger(__name__)


async def check_protein_variation(
    store: ContractStore, best_recipe_ids: list[int], week: int, agreement_id: int
) -> tuple[dict, str]:
    # We need the  main ingredient category for coutning protein varn and thus need to load it.
    regular_recipe_features: pd.DataFrame = await store.feature_view(RecipeFeatures).all().to_pandas()

    main_ingr_pr_wk = {}
    ingr_pr_wk = []
    for main_recipe_id in best_recipe_ids:
        data_of_interest = (
            pd.DataFrame(
                regular_recipe_features[regular_recipe_features["main_recipe_id"] == main_recipe_id][
                    ["main_ingredient_id"]
                ]
            )
            .drop_duplicates()
            .iloc[0]
        )
        ingr_pr_wk.append(data_of_interest)
        main_ingr_pr_wk[week] = ingr_pr_wk

    cleaned_main_ingr_pr_wk = {k: [v_.iloc[0] for v_ in v] for k, v in main_ingr_pr_wk.items()}
    # Computation of duplicate protein percentage using numpy arrays for speed
    dupl_main_ingr_pr_wk = np.array(
        [(len(ingr) - len(set(ingr))) / len(ingr) for ingr in cleaned_main_ingr_pr_wk.values()]
    )
    wks = np.array(list(cleaned_main_ingr_pr_wk.keys()))

    logger.info(f"Duplicate proteins per week: {dict(zip(wks, dupl_main_ingr_pr_wk))}")
    info_duplicate_proteins = dict(zip(wks, dupl_main_ingr_pr_wk))

    threshold = 0.6
    dupl_wks = wks[dupl_main_ingr_pr_wk >= threshold]

    string_info = ""
    if len(dupl_wks) > 0:
        logger.warning(f"Weeks {dupl_wks} have a high proportion of duplicate proteins for AG: {agreement_id}")
        string_info = f"Weeks {dupl_wks} have a high proportion of duplicate proteins for AG: {agreement_id}"

    return info_duplicate_proteins, string_info


async def import_recipe_features(store: ContractStore) -> pd.DataFrame:
    regular_recipe_features: pd.DataFrame = await store.feature_view(RecipeFeatures).all().to_pandas()
    print(type(regular_recipe_features))  # noqa: T201
    print(regular_recipe_features.head())  # noqa: T201
    return regular_recipe_features


if __name__ == "__main__":
    import asyncio

    from dotenv import load_dotenv

    load_dotenv()
    asyncio.run(import_recipe_features(store=preselector_store()))
