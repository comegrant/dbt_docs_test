import logging
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

import pandas as pd
from aligned import FeatureStore
from aligned.retrival_job import ConvertableToRetrivalJob, RetrivalJob

logger = logging.getLogger(__name__)


def preprocessing(data: pd.DataFrame) -> pd.DataFrame:
    """
    Pre process the input data into internal model features.
    Aka. doing a one hot encoding on every instance mentioned in each array column.

    ```python
    features = pd.DataFrame({
        "recipe_taxonomies": ["rask,laktose", "laktose", "barnevenlig"],
        "all_ingredients": ["fisk", "kjøtt,paprika", "gulrot,laks"],
    })
    processed = preprocessing(features)
    >>> print(processed)
    ┌────────────┬─────────────┬──────────┬──────────┬────────────┬───────────┬──────────┬─────────────┐
    │ tax_barnev ┆ tax_laktose ┆ tax_rask ┆ ing_fisk ┆ ing_gulrot ┆ ing_kjøtt ┆ ing_laks ┆ ing_paprika │
    │ enlig      ┆ ---         ┆ ---      ┆ ---      ┆ ---        ┆ ---       ┆ ---      ┆ ---         │
    │ ---        ┆ i64         ┆ i64      ┆ i64      ┆ i64        ┆ i64       ┆ i64      ┆ i64         │
    │ i64        ┆             ┆          ┆          ┆            ┆           ┆          ┆             │
    ╞════════════╪═════════════╪══════════╪══════════╪════════════╪═══════════╪══════════╪═════════════╡
    │ 0          ┆ 1           ┆ 1        ┆ 1        ┆ 0          ┆ 0         ┆ 0        ┆ 0           │
    │ 0          ┆ 1           ┆ 0        ┆ 0        ┆ 0          ┆ 1         ┆ 0        ┆ 1           │
    │ 1          ┆ 0           ┆ 0        ┆ 0        ┆ 1          ┆ 0         ┆ 1        ┆ 0           │
    └────────────┴─────────────┴──────────┴──────────┴────────────┴───────────┴──────────┴─────────────┘
    ```
    """
    data_tax = data["recipe_taxonomies"].str.replace(" ", "").str.get_dummies(sep=",")
    data_ing = data["all_ingredients"].str.replace(" ", "").str.get_dummies(sep=",")

    processed = pd.concat(
        [data_tax.add_prefix(prefix="tax_"), data_ing.add_prefix("ing_")], axis=1
    )
    processed.columns = processed.columns.str.replace(" ", "")
    return processed


@dataclass
class CBModel:
    # The user recipie rating matrix
    matrix: pd.DataFrame
    model_contract_name: str
    model_version: str

    def predict(self, recipes: pd.DataFrame) -> pd.DataFrame:
        """
            Predicting for data in given week
            Dot product of matrix 1 (matrix of userIDs with recipe ingredients + taxonomies of trained model)
                and matrix 2(combined matrix of recipeIDs containing ingredient + taxonomies) to obtain user profile
        Args:
            data_pred ([DF]): combined matrix of recipeIDs containing ingredient + taxonomies (predict data)

        Returns:
            [DF]: DF contains scores of dishes for each customer
        """
        if recipes.shape[0] == 0:
            return pd.DataFrame({"agreement_id": [], "recipe_id": [], "score": []})

        processed_recipe = preprocessing(recipes)
        logger.info("Prepocessing done")

        recipe_data = processed_recipe.drop(
            processed_recipe.columns.difference(list(self.matrix)), axis=1
        )
        user_data = self.matrix[list(recipe_data)]
        logger.info("Selected interesection of features and preds")

        # Getting agreement-recipe matrix with total correlation value
        data_pred = user_data.dot(recipe_data.T)
        logger.info("Completed dot product predictions")

        def normalise(x):
            denum = recipe_data.loc[x.name].sum()
            if (denum != 0).all():
                return x / denum
            else:
                return x

        # Normalization of agreement-recipe total value (using to number of taxonomies and ingredients)

        logger.info("Will normalize predictions")
        normalised = data_pred.apply(normalise)

        logger.info("Normalized. Will melt and return predictions")
        return pd.melt(
            normalised.reset_index(),
            id_vars="agreement_id",
            var_name="recipe_id",
            value_name="score",
        )

    async def predict_over(
        self, menu: pd.DataFrame, store: FeatureStore
    ) -> pd.DataFrame:
        model_store = store.model(self.model_contract_name)
        needed_entities = [entity.name for entity in model_store.needed_entities()]

        for column in needed_entities:
            if column not in menu.columns:
                raise ValueError(
                    f"Missing {needed_entities} in menus. Got {menu.columns}"
                )

        entities = menu[needed_entities].drop_duplicates()

        recipe_features = await model_store.features_for(entities).to_pandas()

        logger.info(
            f"Predicting recommendation ratings for {recipe_features.shape[0]} recipes"
        )

        predicted_at = datetime.utcnow()
        recipe_features.index = recipe_features["recipe_id"]

        preds = self.predict(recipe_features)
        preds["predicted_at"] = predicted_at
        preds["model_version"] = self.model_version

        return preds.dropna()

    @staticmethod
    def train(
        recipes: pd.DataFrame,
        ratings: pd.DataFrame,
        model_contract_name: str,
        model_version: str | None = None,
    ) -> "CBModel":
        if not model_version:
            model_version = str(uuid4())

        if "delivered_at" in ratings.columns:
            ratings = ratings.drop(columns=["delivered_at"])

        processed = preprocessing(recipes)
        joined_ratings = ratings.merge(
            pd.concat([processed, recipes["recipe_id"]], axis=1),
            how="inner",
            on="recipe_id",
        )

        logger.info(f"Training CB Model with {joined_ratings.shape[0]} ratings")

        dynamic_feature_columns = list(
            set(joined_ratings.columns) - {"agreement_id", "recipe_id", "rating"}
        )

        weighted = joined_ratings[dynamic_feature_columns].mul(
            joined_ratings["rating"] ** 3, axis=0
        )
        weighted["agreement_id"] = joined_ratings["agreement_id"]

        summed_per_user = weighted.groupby("agreement_id").sum()
        if summed_per_user.shape[0] == 0:
            raise ValueError(
                "Unable to train CB Model, as no users preferences were produced"
            )
        return CBModel(
            summed_per_user, model_contract_name, model_version=model_version
        )

    @staticmethod
    async def train_using(
        entities: RetrivalJob | ConvertableToRetrivalJob,
        store: FeatureStore,
        model_contract_name: str,
        ratings_view: str,
        agreement_ids_subset: list[int] | None = None,
        model_version: str | None = None,
    ) -> "CBModel":
        from aligned.validation.pandera import PanderaValidator

        recipes = await (
            store.model(model_contract_name)
            .features_for(entities)
            .drop_invalid(PanderaValidator())
            .to_pandas()
        )
        ratings = await (
            store.feature_view(ratings_view)
            .all()
            .drop_invalid(PanderaValidator())
            .to_pandas()
        )

        mean_recipe_rating = ratings.groupby("recipe_id")["rating"].mean().reset_index()
        ratings["rating"] = (
            ratings["rating"]
            .astype("float")
            .fillna(
                ratings[["recipe_id"]].merge(
                    mean_recipe_rating, on="recipe_id", how="left"
                )["rating"]
            )
            .fillna(3)
        )

        if agreement_ids_subset:
            ratings = ratings[ratings["agreement_id"].isin(agreement_ids_subset)]

        return CBModel.train(recipes, ratings, model_contract_name, model_version)
