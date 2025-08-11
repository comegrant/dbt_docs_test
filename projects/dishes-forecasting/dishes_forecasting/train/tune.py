from dishes_forecasting.train.train_pipeline import train_model
import optuna
import pandas as pd
from typing import Optional
from constants.companies import Company
import numpy as np
import itertools


def tune_pipeline(
    company: Company,
    env: str,
    training_set: pd.DataFrame,
    train_config: dict,
    n_trials: Optional[int] = 30,
) -> any:
    def objective(trial): # noqa
        params_lgb = {
            "learning_rate": trial.suggest_float("lgb_learning_rate", 0.1, 0.3),
            "n_estimators": trial.suggest_int("lgb_n_estimators", 700, 1200),
            "num_leaves": trial.suggest_int("lgb_num_leaves", 600, 700),
            "max_depth": trial.suggest_int("lgb_max_depth", 5, 10),
            "min_child_samples": trial.suggest_int("lgb_min_child_samples", 50, 100),
        }
        params_xgb = {
            "learning_rate": trial.suggest_float("xgb_learning_rate", 0.05, 0.2),
            "n_estimators": trial.suggest_int("xgb_n_estimators", 200, 1500),
            "subsample": trial.suggest_uniform("xgb_subsample", 0.5, 1.0),
        }
        params_rf = {
            "n_estimators": trial.suggest_int("rf_n_estimators", 50, 200),
            "max_depth": trial.suggest_int("rf_max_depth", 10, 50),
            "max_features": trial.suggest_int("rf_max_features", 50, 500),
        }

        _, _, _, _, _, _, mae, _, _ = train_model(
            training_set=training_set,
            params_lgb=params_lgb,
            params_xgb=params_xgb,
            params_rf=params_rf,
            company=company,
            env=env,
            train_config=train_config,
            is_log_model=False,
            is_register_model=False,
            is_running_on_databricks=True,
        )
        return mae

    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=n_trials)
    return study.best_params, study.best_value


def grid_search_params(
    company: Company,
    env: str,
    training_set: pd.DataFrame,
    train_config: dict,
) -> None:
    grid_lgb = {
        "learning_rate": np.arange(0.01, 0.21, 0.02).tolist(),
        "n_estimators": list(range(700, 1001, 20)),
        'num_leaves': list(range(500, 701, 100)),
        'max_depth': [3, 5, 9, 15, 20],
    }

    grid_xgb = {
        "learning_rate": [0.1],
        "n_estimators": list(range(700, 1001, 20)),
    }
    grid_rf = {
        "n_estimators": list(range(100, 201, 20)),
        "max_features": list(range(100, 201, 20)),
    }

    combinations = list(itertools.product(
        itertools.product(
            grid_lgb["learning_rate"],
            grid_lgb["n_estimators"],
            grid_lgb["num_leaves"],
            grid_lgb["max_depth"],
        ),
        itertools.product(
            grid_xgb["learning_rate"],
            grid_xgb["n_estimators"],
        ),
        itertools.product(
            grid_rf["n_estimators"],
            grid_rf["max_features"],
        ),
    ))

    for lgb_params, xgb_params, rf_params in combinations:
        lgb_dict = {
            "learning_rate": lgb_params[0],
            "n_estimators": lgb_params[1],
            'num_leaves': lgb_params[2],
            'max_depth': lgb_params[3],
        }
        xgb_dict = {
            "learning_rate": xgb_params[0],
            "n_estimators": xgb_params[1]
        }
        rf_dict = {
            "max_depth": rf_params[0],
            "max_features": rf_params[1]}
        _, _, _, _, _, _, mae, _, _ = train_model(
            training_set=training_set,
            params_lgb=lgb_dict,
            params_xgb=xgb_dict,
            params_rf=rf_dict,
            company=company,
            env=env,
            train_config=train_config,
            is_log_model=False,
            is_register_model=False,
            is_running_on_databricks=True,
        )
        print(mae) # noqa
