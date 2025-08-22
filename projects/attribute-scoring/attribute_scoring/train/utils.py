import logging
from typing import Union

import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
from attribute_scoring.common import ArgsTrain
from attribute_scoring.train.configs import ModelConfig
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

MODEL_CONFIG = ModelConfig()


def check_target_distribution(target: pd.Series) -> None:
    """Logs warnings if there is a class imbalance in the target variable."""
    minority_class_ratio = target.value_counts().min() / len(target)

    if minority_class_ratio <= MODEL_CONFIG.class_imbalance_thresholds["severe"]:
        logging.error(
            f"Severe class imbalance detected: minority class is {minority_class_ratio * 100:.2f}% of the data."
        )
    elif minority_class_ratio <= MODEL_CONFIG.class_imbalance_thresholds["significant"]:
        logging.warning(
            f"Significant class imbalance detected: minority class is {minority_class_ratio * 100:.2f}% of the data."
        )
    elif minority_class_ratio <= MODEL_CONFIG.class_imbalance_thresholds["slight"]:
        logging.info(
            f"Slight class imbalance detected: minority class is {minority_class_ratio * 100:.2f}% of the data."
        )

    else:
        logging.info("No class imbalance detected.")


def log_metrics(
    y_true: Union[np.ndarray, list, pd.Series],
    y_pred: Union[np.ndarray, list, pd.Series],
    cv_results: dict,
) -> float:
    """Logs cross-validation and test set metrics, and returns the chosen evaluation metric."""
    metrics = ["accuracy", "f1", "precision", "recall"]

    for metric in metrics:
        metric_value = cv_results[f"test_{metric}"].mean()
        mlflow.log_metric(f"val_{metric}", metric_value)

    test_metrics = {
        "accuracy": accuracy_score(y_true, y_pred),
        "f1": f1_score(y_true, y_pred),
        "precision": precision_score(y_true, y_pred),
        "recall": recall_score(y_true, y_pred),
    }
    for metric_name, metric_value in test_metrics.items():
        mlflow.log_metric(f"test_{metric_name}", metric_value)

    return test_metrics[MODEL_CONFIG.evaluation_metric]


def log_feature_importance(
    args: ArgsTrain,
    feature_importance: list[float],
    feature_names: list[str],
    top_n: int = 15,
) -> None:
    """Logs and saves a feature importance plot."""
    features_sorted = sorted(
        zip(feature_importance, feature_names), reverse=True, key=lambda x: x[0]
    )
    top_features = features_sorted[:top_n]
    top_importance, top_names = zip(*top_features)

    plt.figure(figsize=(10, 6))
    plt.barh(top_names, top_importance)
    plt.xlabel("Feature Importance")
    plt.ylabel("Features")
    plt.title(f"Top {top_n} Feature Importance {args.company} ({args.target})")
    plt.tight_layout()
    plt.savefig("feature_importance_plot.png")
    mlflow.log_artifact("feature_importance_plot.png")
    plt.close()
