import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

import numpy as np
import pandas as pd
import polars as pl
from aligned import FeatureStore
from aligned.retrival_job import ConvertableToRetrivalJob, RetrivalJob

from rec_engine.logger import Logger

file_logger: Logger = logging.getLogger(__name__)


def preprocessing(data: pd.DataFrame) -> pd.DataFrame:
    """
    Pre process the input data into internal model features.
    Aka. doing a one hot encoding on every instance mentioned in each array column.

    ```python
    features = pd.DataFrame({
        "recipe_taxonomies": ["rask,laktose", "laktose", "barnevenlig"],
        "all_ingredients": ["fisk", "kjøtt,paprika", "gulrot,laks"],
        "other_values": [1, 1, 0],
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
    data_tax = data["recipe_taxonomies"].str.lower().str.replace(" ", "").str.get_dummies(sep=",")
    data_ing = (
        data["all_ingredients"]
        .str.lower()
        .str.replace(" ", "")
        .str.replace("[", "")
        .str.replace("]", "")
        .str.replace('"', "")
        .str.replace(r"\d+pk", "", regex=True)
        .str.replace(r"\d+", "", regex=True)
        .str.get_dummies(sep=",")
    )

    other_columns = list(data.columns)
    other_columns.remove("recipe_taxonomies")
    other_columns.remove("all_ingredients")

    processed = pd.concat(
        [data_tax.add_prefix(prefix="tax_"), data_ing.add_prefix("ing_")],
        axis=1,
    )
    processed.columns = processed.columns.str.replace(" ", "")
    return pd.concat([data[other_columns], processed], axis=1)


@dataclass
class CBModel:
    # The user recipie rating matrix
    matrix: pd.DataFrame
    model_contract_name: str
    model_version: str

    def predict(
        self,
        recipes: pd.DataFrame,
        logger: Logger | None = None,
    ) -> pd.DataFrame:
        """
            Predicting for data in given week
            Dot product of matrix 1 (matrix of userIDs with recipe ingredients + taxonomies of trained model)
                and matrix 2(combined matrix of recipeIDs containing ingredient + taxonomies) to obtain user profile
        Args:
            data_pred ([DF]): combined matrix of recipeIDs containing ingredient + taxonomies (predict data)

        Returns:
            [DF]: DF contains scores of dishes for each customer
        """

        logger = logger or file_logger
        if recipes.shape[0] == 0:
            return pd.DataFrame({"agreement_id": [], "recipe_id": [], "score": []})

        processed_recipe = preprocessing(recipes)
        logger.info("Prepocessing done")

        recipe_data = processed_recipe.drop(
            processed_recipe.columns.difference(list(self.matrix)),
            axis=1,
            inplace=False,
        )
        logger.info("Selecting interesection of features and preds")
        user_data = self.matrix[list(recipe_data)]

        # Getting agreement-recipe matrix with total correlation value
        logger.info("Computing dot product predictions")
        data_pred = user_data.dot(recipe_data.T)

        # Normalization of agreement-recipe total value (using to number of taxonomies and ingredients)

        logger.info("Will melt and return predictions")
        return pd.melt(
            data_pred.reset_index(),
            id_vars="agreement_id",
            var_name="recipe_id",
            value_name="score",
        )

    async def predict_over(
        self,
        menu: pd.DataFrame,
        store: FeatureStore,
        logger: Logger | None = None,
    ) -> pd.DataFrame:
        logger = logger or file_logger
        model_store = store.model(self.model_contract_name)
        # Removing dups
        needed_entities = list({entity.name for entity in model_store.needed_entities()})

        for column in needed_entities:
            if column not in menu.columns:
                raise ValueError(
                    f"Missing {needed_entities} in menus. Got {menu.columns}",
                )

        recipe_features = await model_store.features_for(menu).with_subfeatures().drop_invalid().to_pandas()

        logger.info(
            f"Predicting recommendation ratings for {recipe_features.shape[0]} recipes",
        )

        predicted_at = datetime.now(tz=timezone.utc)
        recipe_features.index = recipe_features["recipe_id"]

        preds = self.predict(recipe_features, logger)
        preds["predicted_at"] = predicted_at
        preds["model_version"] = self.model_version
        preds["year_week"] = np.repeat(
            recipe_features["yearweek"].to_numpy(), int(preds.shape[0] / recipe_features.shape[0])
        )
        return preds.dropna()

    @staticmethod
    def train(
        recipes: pd.DataFrame,
        ratings: pd.DataFrame,
        model_contract_name: str,
        model_version: str | None = None,
        logger: Logger | None = None,
    ) -> "CBModel":
        logger = logger or file_logger

        if not model_version:
            model_version = str(uuid4())

        if "delivered_at" in ratings.columns:
            ratings = ratings.drop(columns=["delivered_at"])

        processed = preprocessing(recipes)

        ratings["recipe_id"] = ratings["recipe_id"].astype("int32")

        logger.info("Preprocessing done")

        joined_ratings = ratings[["recipe_id", "rating", "agreement_id"]].merge(
            processed,
            how="inner",
            on="recipe_id",
        )

        logger.info(f"Training CB Model with {joined_ratings.shape[0]} ratings")
        dynamic_feature_columns = list(
            set(joined_ratings.columns) - {"agreement_id", "recipe_id", "rating"},
        )

        weighted = joined_ratings[dynamic_feature_columns].mul(
            joined_ratings["rating"] ** 3,
            axis=0,
        )

        def inverse_frequency(x: pd.Series) -> pd.Series:
            inv_feq_smooth = np.log(x.shape[0] / (1 + x[x > 0].count())) + 1
            return x * inv_feq_smooth

        weighted = weighted.apply(inverse_frequency, axis=0)
        weighted["agreement_id"] = joined_ratings["agreement_id"]

        summed_per_user = weighted.groupby("agreement_id").sum()
        if summed_per_user.shape[0] == 0:
            raise ValueError(
                "Unable to train CB Model, as no users preferences were produced",
            )

        return CBModel(
            summed_per_user,
            model_contract_name,
            model_version=model_version,
        )

    @staticmethod
    async def train_using(
        entities: RetrivalJob | ConvertableToRetrivalJob,
        store: FeatureStore,
        model_contract_name: str,
        explicit_rating_view: str,
        model_version: str | None = None,
        logger: Logger | None = None,
    ) -> "CBModel":
        origin_value = 3
        if logger is None:
            logger = file_logger

        logger.info("Loading recipes and ratings")
        if isinstance(entities, pl.LazyFrame):
            recipe_entities = entities.select("recipe_id").unique("recipe_id")
        elif isinstance(entities, pd.DataFrame):
            recipe_entities = entities[["recipe_id"]].drop_duplicates()
        else:
            recipe_entities = entities

        recipes = await store.model(model_contract_name).features_for(recipe_entities).drop_invalid().to_pandas()
        logger.info(f"Loaded {recipes.shape[0]} recipes")
        ratings = (
            await store.feature_view(explicit_rating_view)
            .select({"rating"})
            .features_for(entities)
            .drop_invalid()
            .to_pandas()
        )
        ratings["rating"] = (ratings["rating"] - origin_value) * 2
        logger.info(f"Loaded {ratings.shape[0]} ratings")

        logger.info("Loading behavioral rating")
        behavioral_rating_pl = await store.feature_view("mealbox_changes_as_rating").all().to_lazy_polars()
        behavioral_rating_pl = behavioral_rating_pl.filter(
            pl.col("agreement_id").is_in(entities["agreement_id"].unique()),
        ).with_columns((pl.col("rating") + 2) - origin_value)
        behavioral_rating = behavioral_rating_pl.collect().to_pandas()

        logger.info(f"Loaded {behavioral_rating.shape[0]} behavioral ratings")

        ratings = pd.concat([ratings, behavioral_rating], axis=0)

        logger.info("Filling in missing ratings")
        ratings["rating"] = ratings["rating"].astype("float")

        ratings = ratings[~ratings["rating"].isnull()]

        logger.info("Training model")
        return CBModel.train(
            recipes,
            ratings,
            model_contract_name,
            model_version,
            logger=logger,
        )
