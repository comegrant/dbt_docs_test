# 🧾🤖 Orders Forecast ML

A Machine learning algorithm for forecasting total orders for the upcoming 11/15 weeks

<h2>❓What does it do</h2>

This project trains a machine learning model to predict the total order volume and the total number of flex orders for the coming weeks. On a high level, it performs the tasks below

1. Download necessary data
2. Prepare dataset for machine learning
3. Train and predict
4. Upload outputs to databricks

<h2> 🧮 Model details</h2>

<h3> Input data</h3>

1. [Order history](https://adb-4291784437205825.5.azuredatabricks.net/explore/data/dev/mltesting/orders_weekly_aggregated?o=4291784437205825) : fetches order volume (unique number of customers) of total orders, mealbox orders and standalone dishes orders. The resulting dataframe contains the following columns:
    ```
        - year: int
        - week: int
        - company_id: string (uuid)
        - num_total_orders: int       # total order volume
        - num_mealbox_orders: int     # order volume of mealboxes
        - num_dishes_orders: int      # order volume of stand alone dishes
        - perc_dishes_orders: float   # num_dishes_orders / num_total_orders
    ```
2. [Estimations: total](https://adb-4291784437205825.5.azuredatabricks.net/explore/data/dev/mltesting/estimations_total?o=4291784437205825): Fetches the estimations of total order volume, on a daily cadence. That is to say, every day, we fetch 1 record (at hour 0 each day) of estimation for all available future weeks. For each timestamp, there are typically 12 weeks of estimations available. The dataframe contains the following columns:
    ```
        - timestamp: Datetime              # timestamp of estimation
        - year: int                        # the year to estimate for (future)
        - week: int                        # the week to estimate for (future)
        - product_type_id: string (uuid)
        - quantity_estiamted_total: int    # estimate of total order volume
    ```
3. [Estimations: dishes](https://adb-4291784437205825.5.azuredatabricks.net/explore/data/dev/mltesting/estimations_dishes?o=4291784437205825): Fetches the estimations of total order volume, on a daily cadence. Similar logic as estimations total, but we need to fetch the data using the fake ``variation_id = "10000000-0000-0000-0000-000000000000"``. This query fetches the number of orders that contain stand-alone dishes, not the total amount of stand alone dishes. The dataframe contains the following columns:
    ```
        - timestamp: Datetime               # timestamp of estimation
        - year: int                         # the year to estimate for (future)
        - week: int                         # the week to estimate for (future)
        - product_type_id: string (uuid)
        - quantity_estiamted_dishes: int    # estimate of number of flex orders
    ```

<h3> Transformations and Features</h3>

1. Preliminary cleaning

    - For each estimation date, year, and week, we only keep 1 record (according to estimation_timestamp) if there are duplicates.

    - The data from estimations log sometimes contains week 53 when the year does not have week 53 according to the ISO standard. We remove those data points.

    - In order history, we remove some potential outlier data, identified by ``num_total_orders <50 ``. We will forward fill missing data

    - Add a date column ``first_date_of_week``,defined as the date of the Monday for each delivery year-week

    - Augment estimations: if there are not enough weeks exists in estimations log, we will back fill the missing weeks. This is especially relevant when running for 15 week forecasts in the summer.

2. Features

    - [number of days to cut off](https://dev.azure.com/godtlevertgruppenas/Analytics/_git/Forecasting?path=/forecasting/ml/orders/orders_forecasting/features.py&version=GBmain&line=124&lineEnd=124&lineStartColumn=6&lineEndColumn=31&lineStyle=plain&_a=contents): the difference between ``estimation_date``, and the cut off day of the delivery year-week. If this value is smaller than 0, they are treated as irregular data points and will be removed.

    - [weekly seasonality](https://dev.azure.com/godtlevertgruppenas/Analytics/_git/Forecasting?path=/forecasting/ml/orders/orders_forecasting/features.py&version=GBmain&line=166&lineEnd=166&lineStartColumn=5&lineEndColumn=23&lineStyle=plain&_a=contents): obtained by first detrend each week's order volume by subtracting the 52 week moving average. And then aggregate detrended value by week. This is one of the most important features for the model

    - [retention projection](https://dev.azure.com/godtlevertgruppenas/Analytics/_git/Forecasting?path=/forecasting/ml/orders/orders_forecasting/features.py&version=GBmain&line=179&lineEnd=183&lineStartColumn=4&lineEndColumn=28&lineStyle=plain&_a=contents): Calculate the percentage of estimated customers that will place an order, for each num_days_to_cut_off, and multiply the retention rate to the target (num_total_orders or num_dishes_orders)

    - [moving averages and lags](https://dev.azure.com/godtlevertgruppenas/Analytics/_git/Forecasting?path=/forecasting/ml/orders/orders_forecasting/pipeline.py&version=GBmain&line=170&lineEnd=178&lineStartColumn=4&lineEndColumn=6&lineStyle=plain&_a=contents): 12-weeks and 15-weeks moving average and lags

    - [holiday features](https://dev.azure.com/godtlevertgruppenas/Analytics/_git/Forecasting?path=/forecasting/ml/orders/orders_forecasting/features.py&version=GBmain&line=136&lineEnd=136&lineStartColumn=5&lineEndColumn=80&lineStyle=plain&_a=contents): get country's holiday calendar, and one-hot-encode all holidays. If the holiday is on a Friday or Monday, then we will have one more binary feature ``is_long_weekend`` set to true

    - Removal of Covid dates: the covid days sales data have a potential of messing up Easter features, so they are simply removed in the end before train/val/test split.

3. Train/Val/Test split
    - Validation/holdout set is defined as the data with ``estimation_date`` equals the first available date before ``prediction_date``. If there are not enough target data points on the ``holdout_set_date`` defined as the point above, for example, when ``prediction_date`` is set to today, we use the last 50 rows of data where the target is known as the validation set
    - Training set: contains data from the first available ``estimation_date`` to 1 day before ``hold_out_set_date``
    - Test set: contains data with the ``estimation_date`` equals ``prediction_date``

<h3> Model </h3>

1. Ensemble models

    We use the ensemble features from the ``pycaret`` library. We provide a list of models for the ensembler to try out, and then the top 2 performing models' outputs are aggregated to provide the final prediction. Usually the top performing models are ``Extra Trees Regressor``, ``LightGBM`` and ``XGBoost``.

2. Targets
    We train the model with the same features for three different targets:
    1. num_total_orders
    2. num_dishes_orders
    3. perc_dishes_orders

    The perc_dishes_orders are converted back to number of dishes by multiplying with the predicted number_total_orders. We take the average outputs of 2.2 and 2.3 are taken as the final prediction for the number of dishes orders

3. ML experiments logging

    Each run of the ML model is logged in ML flow. See [here](https://adb-4291784437205825.5.azuredatabricks.net/ml/experiments/2959266603557795?o=4291784437205825&searchFilter=&orderByKey=attributes.start_time&orderByAsc=false&startTime=ALL&lifecycleFilter=Active&modelVersionFilter=All+Runs&datasetsFilter=W10%3D) for one example of logged artifacts. ``MAPE`` is the main error metric for this model.


<h2>🏃‍♀️How to run it locally?</h2>

```
cd path/to/repo/sous-chef/projects/orders-forecasting
```
