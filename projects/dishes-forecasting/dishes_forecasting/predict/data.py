import logging

import pandas as pd
from constants.companies import Company

from dishes_forecasting.train.configs.feature_lookup_config import FeatureLookUpConfig
from dishes_forecasting.schema import feature_schema
from catalog_connector import connection


def create_pred_dataset(
    min_yyyyww: int,
    company: Company,
    feature_lookup_config_list: FeatureLookUpConfig,
    schema: str = "mlgold",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_left = download_weekly_variations(
        company=company,
        min_yyyyww=min_yyyyww,
        schema=schema,
    )
    df_list = []
    ignore_columns = []
    for a_feature_lookup_config in feature_lookup_config_list:
        feature_container = a_feature_lookup_config.features_container
        feature_table_name = a_feature_lookup_config.feature_table_name
        feature_columns = a_feature_lookup_config.primary_keys + a_feature_lookup_config.feature_columns
        table_name = f"{feature_container}.{feature_table_name}"
        logging.info(f"Downloading data from {table_name} ...")
        sql = f"""
        select {",".join(feature_columns)} from {table_name}
        """
        df = connection.sql(sql).toPandas()
        df_list.append(df)
        ignore_columns.extend(a_feature_lookup_config.exclude_in_training_set)
    ignore_columns = list(set(ignore_columns))

    df_merged = df_left
    for df in df_list:
        df_merged = df_merged.merge(df, how="left")
    df_merged = df_merged.dropna()
    df_merged = df_merged.drop_duplicates(
        subset=["menu_year", "menu_week", "product_variation_id"]
    ).reset_index().drop(columns=["index"])
    feature_schema.coerce = True
    df_merged = feature_schema.validate(df_merged)
    return df_merged, df_left


def download_weekly_variations(
    company: Company,
    min_yyyyww: int,
    schema: str = "mlgold",
) -> pd.DataFrame:
    company_id = company.company_id
    df_left = connection.sql(
        f"""
        select
            menu_year,
            menu_week,
            company_id,
            product_variation_id
        from {schema}.dishes_forecasting_weekly_dishes_variations
        where company_id = '{company_id}'
        and menu_year * 100 + menu_week >= {min_yyyyww}
        order by menu_year, menu_week
        """
    ).toPandas()

    return df_left
