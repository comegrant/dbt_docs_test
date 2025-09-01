# 🍛 Dishes Forecasting

A machine learning algorithm for forecasting stand alone dishes quantity


## 🏋️‍♀️ train parameters

- company: LMK, AMK, GL, RT
- env: dev, test, prod
- is_running_on_databricks: true, false. Set this to be True if you are running on the databricks cluster. This is related to the mlflow tracking uri.

## 🔮 predict parameters

- company: LMK, AMK, GL, RT
- env: dev, test, prod
- forecast_date: YYYY-MM-DD. If not provided, the current date will be used.
- is_running_on_databricks: true, false
- is_normalize_predictions: true, false. If true, the predictions will be normalized such that the ratio of one week's predictions sum to 1. This will be ignored if the is_modify_new_portions parameter is set to true.
- is_modify_new_portions: true, false. If true, the predictions will be modified to add new portions and targets.
- is_add_new_portions_on_top: true, false. If true, the new portions will be added to the top of the predictions.
- new_portions: list of integers. The new portions to be added. Format must be a comma-separated list of integers. For example, "3, 5"
- new_portions_targets: list of floats. The targets for the new portions. Format must be a comma-separated list of floats. For example, "0.05, 0.10"
- profile_name: str. The profile name to be used for the mlflow tracking. This is relevant if you are running mlflow locally and want to use your own profile for authentication.
