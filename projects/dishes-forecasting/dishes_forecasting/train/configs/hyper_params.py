def load_hyperparams(
    company: str
) -> tuple[dict, dict]:
    if company == "GL":
        param_class = HyperParamsGL
    elif company == "AMK":
        param_class = HyperParamsAMK
    elif company == "LMK":
        param_class = HyperParamsLMK
    elif company == "RT":
        param_class = HyperParamsRT
    params_lgb = param_class.params_lgb
    params_rf = param_class.params_rf
    params_xgb = param_class.params_xgb
    return params_lgb, params_rf, params_xgb


class HyperParamsGL:
    params_lgb = {
        'n_estimators': 894,
        'learning_rate': 0.26632616409631515,
        'num_leaves': 643,
        'max_depth': 9,
        'min_child_samples': 71,
        'subsample': 0.6602672349948788,
        'colsample_bytree': 0.6410951732786723
    }

    params_xgb = {
        'max_depth': 9,
        'learning_rate': 0.1092565364548268,
        'n_estimators': 923,
        'min_child_weight': 5,
        'subsample': 0.751830284232397,
        'colsample_bytree': 0.7259923661996679
    }
    params_rf = {
        'n_estimators': 167,
        'max_depth': 22,
        'min_samples_split': 2,
        'min_samples_leaf': 1,
        'max_features': 188
    }


class HyperParamsAMK:
    params_lgb = {
        'n_estimators': 894,
        'learning_rate': 0.26632616409631515,
        'num_leaves': 643,
        'max_depth': 9,
        'min_child_samples': 71,
        'subsample': 0.6602672349948788,
        'colsample_bytree': 0.6410951732786723
    }

    params_xgb = {
        'max_depth': 9,
        'learning_rate': 0.1092565364548268,
        'n_estimators': 923,
        'min_child_weight': 5,
        'subsample': 0.751830284232397,
        'colsample_bytree': 0.7259923661996679
    }
    params_rf = {
        'n_estimators': 167,
        'max_depth': 22,
        'min_samples_split': 2,
        'min_samples_leaf': 1,
        'max_features': 188
    }


class HyperParamsLMK:
    params_lgb = {
        'n_estimators': 894,
        'learning_rate': 0.26632616409631515,
        'num_leaves': 643,
        'max_depth': 9,
        'min_child_samples': 71,
        'subsample': 0.6602672349948788,
        'colsample_bytree': 0.6410951732786723
    }

    params_xgb = {
        'max_depth': 9,
        'learning_rate': 0.1092565364548268,
        'n_estimators': 923,
        'min_child_weight': 5,
        'subsample': 0.751830284232397,
        'colsample_bytree': 0.7259923661996679
    }
    params_rf = {
        'n_estimators': 167,
        'max_depth': 22,
        'min_samples_split': 2,
        'min_samples_leaf': 1,
        'max_features': 188
    }


class HyperParamsRT:
    params_lgb_ = {
        'n_estimators': 766,
        'max_depth': 14,
        'learning_rate': 0.06068161097788177,
        'num_leaves': 179,
        'min_child_samples': 23,
        'subsample': 0.521830724879466,
        'colsample_bytree': 0.916045163812314
    }

    params_lgb = {
        'n_estimators': 894,
        'learning_rate': 0.26632616409631515,
        'num_leaves': 643,
        'max_depth': 9,
        'min_child_samples': 71,
        'subsample': 0.6602672349948788,
        'colsample_bytree': 0.6410951732786723
    }

    params_xgb = {
        'max_depth': 9,
        'learning_rate': 0.1092565364548268,
        'n_estimators': 923,
        'min_child_weight': 5,
        'subsample': 0.751830284232397,
        'colsample_bytree': 0.7259923661996679
    }

    params_rf = {
        'n_estimators': 167,
        'max_depth': 22,
        'min_samples_split': 2,
        'min_samples_leaf': 1,
        'max_features': 188
    }

    params_lgb_best = {
        'n_estimators': 651,
        'learning_rate': 0.05757702613094731,
        'num_leaves': 2766,
        'max_depth': 11,
        'min_child_samples': 59,
        'subsample': 0.7895672019820369,
        'colsample_bytree': 0.9265632248357152
    }
