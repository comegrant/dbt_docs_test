from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

import pandas as pd
from aligned import FeatureStore
from aligned.retrival_job import ConvertableToRetrivalJob, RetrivalJob
from sklearn.cluster import KMeans
from sklearn.decomposition import KernelPCA
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer


def preprocessing_cluster(data: pd.DataFrame) -> pd.DataFrame:
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
    return data["recipe_taxonomies"].str.replace(" ", "").str.get_dummies(sep=",")


@dataclass
class ClusterModel:
    model_version: str
    pipeline: Pipeline
    model_contract_name: str

    def predict(self, data: pd.DataFrame) -> pd.DataFrame:
        return self.pipeline.predict(data)

    async def predict_over(
        self,
        recipes: pd.DataFrame,
        store: FeatureStore,
    ) -> pd.DataFrame:
        entities = recipes[["recipe_id", "year", "week"]].drop_duplicates()
        data = (
            await store.model(self.model_contract_name)
            .features_for(entities)
            .to_pandas()
        )

        predicted_at = datetime.now(tz=UTC)

        preds = entities.copy()
        preds["cluster"] = self.predict(data)
        preds["predicted_at"] = predicted_at
        preds["model_version"] = self.model_version

        return preds.dropna()

    @staticmethod
    def train(
        data: pd.DataFrame,
        model_contract_name: str,
        model_version: str | None = None,
    ) -> "ClusterModel":
        model = Pipeline(
            [
                ("one-hot-encode", FunctionTransformer(preprocessing_cluster)),
                ("pca", KernelPCA(n_components=2, kernel="cosine")),
                (
                    "kmeans",
                    KMeans(
                        n_clusters=4,
                        init="k-means++",
                        max_iter=300,
                        n_init=10,
                        random_state=0,
                    ),
                ),
            ],
        )
        model.fit(data)

        return ClusterModel(
            pipeline=model,
            model_version=model_version or str(uuid4()),
            model_contract_name=model_contract_name,
        )

    @staticmethod
    async def train_using(
        entities: RetrivalJob | ConvertableToRetrivalJob,
        store: FeatureStore,
        model_contract: str,
        model_version: str | None = None,
    ) -> "ClusterModel":
        recipe_features = await (
            store.model(model_contract).features_for(entities).to_pandas()
        )

        return ClusterModel.train(recipe_features, model_contract, model_version)
