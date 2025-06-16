from __future__ import annotations

from aligned import ContractStore, FeatureLocation


def needed_environment_vars(store: ContractStore) -> list[str]:
    from aligned.config_value import EnvironmentValue

    all_configs: list[EnvironmentValue] = []
    for view_name in store.feature_views:
        all_configs.extend(
            config
            for config in store.needed_configs_for(FeatureLocation.feature_view(view_name))
            if isinstance(config, EnvironmentValue)
        )

    for model_name in store.models:
        all_configs.extend(
            config
            for config in store.needed_configs_for(FeatureLocation.model(model_name))
            if isinstance(config, EnvironmentValue)
        )

    return list({env_var.env for env_var in all_configs})
