import logging

import polars as pl
from aligned.compiler.feature_factory import StaticFeatureTags

from data_contracts.preselector.basket_features import BasketFeatures
from data_contracts.preselector.store import SuccessfulPreselectorOutput
from data_contracts.unity_catalog import UCTableSource

logger = logging.getLogger(__name__)


async def migrate_preselector_error_and_taste_preferences() -> None:
    source = SuccessfulPreselectorOutput.metadata.source

    assert isinstance(source, UCTableSource)

    source = source.overwrite_schema()

    schema = await source.schema()
    if "taste_preference_ids" in schema:
        logger.info("Found taste preference ids in the table. Will therefore skip this migration.")
        return

    df = await SuccessfulPreselectorOutput.metadata.source.all_columns().to_polars()
    error_features = [
        feat.name for feat
        in BasketFeatures.query().request.all_returned_features
        if StaticFeatureTags.is_entity not in (feat.tags or [])
    ]
    error_vector_type = pl.Struct({
        feat: pl.Float64
        for feat in error_features
    })
    def decode_broken_taste_preferences(taste_preferences: str) -> list[str]:
        preferences: list[str] = []

        for preference in taste_preferences:
            stripped = preference.removeprefix("{").removesuffix("}")

            components = stripped.split(",")
            assert len(components) == 2 # noqa
            preferences.append(components[0].strip("\""))

        return preferences

    new_df = df.with_columns(
        taste_preference_ids=pl.col("taste_preferences").map_elements(decode_broken_taste_preferences)
    ).with_columns(
        pl.col("error_vector").cast(error_vector_type)
    )
    await SuccessfulPreselectorOutput.query().store.update_source_for(
        SuccessfulPreselectorOutput.location,
        source
    ).overwrite(
        SuccessfulPreselectorOutput.location,
        new_df
    )
