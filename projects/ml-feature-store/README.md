# ML Feature Store

A central depository for feature tables used for ml projects.
We use this repo to perform transformations that are not conveniently performed using SQL but easier using Python, and save them as feature tables in Databricks.


# General Guidelines
1. One folder per table.
2. Start with a dbt model in mlgold. Perform as much transformations in dbt as possible before implementing transformations in Python.
3. Add modular functions to create each feature.
4. Use the prefix `ft_` to indicate that this is a feature table.

# Recommendations
1. When creating a new feature table, fill in the `ml_feature_store/feature_tables.py` with the basic information such as

    - table_name: name of the feature table
    - list of primary keys
    - the dbt model name which the feature table is built upon and the corresponded dbt model schema
    - Example:
        ```
        ft_weekly_dishes_variations_configs = FeatureTable(
            feature_table_name='ft_weekly_dishes_variations',
            primary_keys=['year', 'week', 'company_id', 'product_variation_id'],
            dbt_model_name='weekly_dishes_variations'
        )
        ```
2. Use the common function to load data and load the feature table config
    ```
    from ml_feature_store.feature_tables import ft_weekly_dishes_variations_configs
    from ml_feature_store.common.data import get_data_from_catalog
    from ml_feature_store.common.spark_context import create_spark_context

    spark = create_spark_context()
    table_config = ft_weekly_dishes_variations_configs
    df = get_data_from_catalog(
        spark=spark,
        env=args.env,
        table_name=table_config.dbt_model_name,
        schema=table_config.dbt_model_schema,
        is_convert_to_pandas=True
    )
    df = get_data_from_catalog(
        spark=spark,
        env="dev",
        table_name=table_config["dbt_model_name"],
        schema=table_config["dbt_model_schema"],
        is_convert_to_pandas=True
    )
    ```
