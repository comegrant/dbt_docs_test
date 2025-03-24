import logging

from aligned import FeatureStore

logger = logging.getLogger(__name__)


def custom_store() -> FeatureStore:
    from data_contracts.preselector.store import preselector_contracts
    from data_contracts.recommendations.store import recommendation_feature_contracts

    return preselector_contracts().combined_with(recommendation_feature_contracts())


if __name__ == "__main__":
    from pathlib import Path

    Path("contract_store.json").write_text(
        custom_store().repo_definition().to_json()  # type: ignore
    )
