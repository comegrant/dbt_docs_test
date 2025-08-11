def load_hyperparams(company: str) -> tuple[dict, dict]:
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
        "learning_rate": 0.23794062470986063,
        "max_depth": 9,
        "min_child_samples": 52,
        "n_estimators": 1058,
        "num_leaves": 698,
    }

    params_xgb = {
        "learning_rate": 0.10525604420884736,
        "n_estimators": 930,
        "subsample": 0.6700335622185182,
    }
    params_rf = {"n_estimators": 92, "max_depth": 39, "max_features": 498}


class HyperParamsAMK:
    params_lgb = {
        "learning_rate": 0.01,
        "max_depth": 3,
        "n_estimators": 700,
        "num_leaves": 500,
    }

    params_xgb = {
        "learning_rate": 0.1,
        "n_estimators": 700,
    }
    params_rf = {
        "max_depth": 140,
        "max_features": 120,
    }


class HyperParamsLMK:
    params_lgb = {
        "learning_rate": 0.2314331583552361,
        "max_depth": 9,
        "min_child_samples": 65,
        "n_estimators": 991,
        "num_leaves": 683,
    }

    params_xgb = {
        "learning_rate": 0.10268411379813003,
        "n_estimators": 740,
        "subsample": 0.8581520605632096,
    }
    params_rf = {
        "max_depth": 13,
        "max_features": 66,
        "n_estimators": 196,
    }


class HyperParamsRT:
    params_lgb = {
        "learning_rate": 0.24098320757282524,
        "max_depth": 5,
        "min_child_samples": 51,
        "n_estimators": 775,
        "num_leaves": 678,
    }

    params_xgb = {
        "learning_rate": 0.1201818161935163,
        "n_estimators": 484,
        "subsample": 0.6144915543510929,
    }

    params_rf = {
        "max_depth": 26,
        "max_features": 187,
        "n_estimators": 77,
    }
