import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

from reci_pick.helpers import get_dict_values_as_array, get_dict_values_as_list
from reci_pick.predict.configs.predict_configs import CompanyPredictConfigs
from reci_pick.recipe_preprocessor import RecipePreprocessorModel
from reci_pick.train.configs.train_configs import CompanyTrainConfigs


def preprocess_recipes_dataframe(
    df_recipes: pd.DataFrame,
    company_configs: CompanyTrainConfigs | CompanyPredictConfigs,
    fitted_recipe_transformer: RecipePreprocessorModel | None = None,
) -> tuple[pd.DataFrame, RecipePreprocessorModel]:
    if fitted_recipe_transformer is None:
        fitted_recipe_transformer = RecipePreprocessorModel(
            impute_and_normalize_columns=company_configs.impute_and_normalize_columns,
            impute_and_one_hot_columns=company_configs.impute_and_one_hot_columns,
            impute_only_columns=company_configs.impute_only_columns,
        )
        fitted_recipe_transformer.fit(df_recipes=df_recipes)
    df_transformed = fitted_recipe_transformer.predict(df_recipes=df_recipes)
    non_numeric_columns = ["main_recipe_id", "recipe_name"]
    numeric_feature_columns = company_configs.recipe_numeric_features
    df_transformed[numeric_feature_columns] = df_transformed[numeric_feature_columns].astype(float)
    df_transformed = df_transformed[non_numeric_columns + numeric_feature_columns]
    return df_transformed, fitted_recipe_transformer


def split_train_test(
    df_order_history: pd.DataFrame,
    num_prediction_weeks: int | None = 6,
    split_yyyyww: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    if split_yyyyww is None:
        split_yyyyww = pd.Series(df_order_history["menu_yyyyww"].unique()).nlargest(num_prediction_weeks).iloc[-1]

    df_order_history_train = df_order_history[df_order_history["menu_yyyyww"] < split_yyyyww]
    df_order_history_test = df_order_history[df_order_history["menu_yyyyww"] >= split_yyyyww]

    return df_order_history_train, df_order_history_test, split_yyyyww


def encode_recipe_names(df_recipes: pd.DataFrame) -> tuple[dict, dict]:
    model = SentenceTransformer("sentence-transformers/LaBSE")
    sentence_embeddings = model.encode(df_recipes["recipe_name"])
    id_to_embedding_lookup = dict(zip(df_recipes["main_recipe_id"], sentence_embeddings))
    id_to_name_lookup = dict(zip(df_recipes["main_recipe_id"], df_recipes["recipe_name"]))
    return id_to_embedding_lookup, id_to_name_lookup


def get_top_n_dishes_per_concept(
    df_order_history: pd.DataFrame,
    top_n: int = 10,
    look_back_weeks: int = 12,
    split_yyyyww: int | None = None,
) -> pd.DataFrame:
    """
    Get the top n dishes per concept
    """
    if split_yyyyww is None:
        split_yyyyww = pd.Series(df_order_history["menu_yyyyww"].unique()).nlargest(look_back_weeks).iloc[-1]

    history_last_look_back_weeks = df_order_history[df_order_history["menu_yyyyww"] >= split_yyyyww]
    df_last_look_back_weeks_exploded = history_last_look_back_weeks.explode("concept_combination_list")
    df_dishes_popularity = pd.DataFrame(
        df_last_look_back_weeks_exploded.groupby("concept_combination_list")[
            ["recipe_name", "main_recipe_id"]
        ].value_counts()
    ).reset_index()
    top_n_dishes_indices = (
        df_dishes_popularity.groupby("concept_combination_list")["count"]
        .nlargest(top_n)
        .reset_index()["level_1"]
        .values
    )
    df_top_n_dishes_per_concept = df_dishes_popularity.loc[top_n_dishes_indices]
    df_top_n_dishes_per_concept = df_top_n_dishes_per_concept.rename(columns={"concept_combination_list": "concept"})
    return df_top_n_dishes_per_concept


def get_user_concept_dishes(
    df_order_history_train: pd.DataFrame,
    top_n_per_user: int = 5,
    top_n_per_concept: int = 8,
    look_back_weeks: int = 24,
    df_user_preferences: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    We'll sample top_n_per_user number of popular dishes,
    from top_n_per_concept candidate for each user.
    If user has multiple preference combo,
    we'll sample from the union of all popular concept dishes.
    """
    if df_user_preferences is None:
        df_user_preferences = df_order_history_train[
            ["billing_agreement_id", "concept_combination_list"]
        ].drop_duplicates(subset="billing_agreement_id", keep="last")
    df_user_preferences = df_user_preferences.explode("concept_combination_list").rename(
        columns={"concept_combination_list": "concept"}
    )
    df_top_n_dishes_per_concept = get_top_n_dishes_per_concept(
        df_order_history=df_order_history_train,
        top_n=top_n_per_concept,
        look_back_weeks=look_back_weeks,
    )
    df_user_preferences = df_user_preferences.merge(df_top_n_dishes_per_concept)
    df_user_preferences = df_user_preferences.sample(frac=1)
    df_user_preferences = (
        df_user_preferences.groupby("billing_agreement_id").head(top_n_per_user).sort_values(by="billing_agreement_id")
    )
    df_concept_dishes = pd.DataFrame(
        df_user_preferences.groupby("billing_agreement_id")["main_recipe_id"].value_counts()
    ).reset_index()
    df_concept_dishes = (
        df_concept_dishes.groupby("billing_agreement_id")["main_recipe_id"]
        .agg(list)
        .reset_index()
        .rename(columns={"main_recipe_id": "popular_concept_recipe_ids"})
    )

    return df_concept_dishes


def get_negative_recipes(
    df_order_history: pd.DataFrame,
    df_menu_recipes: pd.DataFrame,
) -> pd.DataFrame:
    # 1. to get which weeks the users have placed an order
    df_user_menu_weeks = pd.DataFrame(
        df_order_history.groupby(["billing_agreement_id"])["menu_yyyyww"].value_counts()
    ).reset_index()

    df_user_menu_weeks_agged = pd.DataFrame(
        df_user_menu_weeks.groupby("billing_agreement_id")["menu_yyyyww"].agg(list)
    ).reset_index()

    # 2. To construct a dictionary that maps week numbers to the main_recipe_ids
    df_menu_recipes_agged = pd.DataFrame(
        df_menu_recipes.groupby("menu_yyyyww")["main_recipe_id"].agg(list)
    ).reset_index()
    week_to_recipe_dict = df_menu_recipes_agged.set_index("menu_yyyyww")["main_recipe_id"].to_dict()

    # 3. To look for all the recipes a user is exposed to: all the recipes on the menu weeks
    # where the user has placed an order
    recipes_exposed = []
    for i in tqdm(df_user_menu_weeks_agged.itertuples()):
        recipe_id_list = get_dict_values_as_list(
            look_up_dict=week_to_recipe_dict, key_list=i.menu_yyyyww, is_flatten_list=True, unique_values_only=True
        )
        recipes_exposed.append(recipe_id_list)
    df_user_menu_weeks_agged["recipes_exposed"] = recipes_exposed

    # 4. Aggregate the recipes that the users that purchased
    df_recipes_ordered_by_user = pd.DataFrame(
        df_order_history.groupby(["billing_agreement_id", "menu_yyyyww"])["main_recipe_id"].value_counts()
    ).reset_index()

    df_recipes_ordered_by_user_agged = pd.DataFrame(
        df_recipes_ordered_by_user.groupby(["billing_agreement_id"])["main_recipe_id"].agg(list)
    ).reset_index()
    df_recipes_ordered_by_user_agged = df_recipes_ordered_by_user_agged.rename(
        columns={"main_recipe_id": "recipes_purchased"}
    )

    df_user_negative_recipes = df_user_menu_weeks_agged.merge(df_recipes_ordered_by_user_agged)

    # 5. Calculate which recipes the users have been exposed to but not purhcased
    df_user_negative_recipes["recipes_not_purchased"] = df_user_negative_recipes["recipes_exposed"].apply(
        lambda x: set(x)
    ) - df_user_negative_recipes["recipes_purchased"].apply(lambda x: set(x))
    df_user_negative_recipes["recipes_not_purchased"] = df_user_negative_recipes["recipes_not_purchased"].apply(
        lambda x: list(x)
    )
    df_user_negative_recipes["num_recipes_purchased"] = df_user_negative_recipes["recipes_purchased"].apply(
        lambda x: len(x)
    )
    df_user_negative_recipes["num_recipes_not_purchased"] = df_user_negative_recipes["recipes_not_purchased"].apply(
        lambda x: len(x)
    )

    return df_user_negative_recipes


def get_user_embeddings_dict(
    df_order_history: pd.DataFrame,
    id_to_embedding_lookup: dict,
    is_pad_popular_recipes: bool = True,
    min_recipes_per_user: int = 5,
) -> dict:
    df_recipes_per_user = pd.DataFrame(
        df_order_history.groupby(["billing_agreement_id"])["main_recipe_id"].value_counts()
    ).reset_index()

    df_recipes_per_user["main_recipe_id"] = df_recipes_per_user.apply(
        lambda row: [row["main_recipe_id"]] * row["count"], axis=1
    )
    df_recipes_per_user_aggregated = (
        df_recipes_per_user.groupby("billing_agreement_id")["main_recipe_id"].agg(sum).reset_index()
    )
    if is_pad_popular_recipes:
        df_recipes_per_user_aggregated = pad_popular_recipe(
            df_recipes_per_user_aggregated=df_recipes_per_user_aggregated,
            df_order_history_train=df_order_history,
            min_recipes_per_user=min_recipes_per_user,
        )
    user_embedings_dict = {}
    for i in df_recipes_per_user_aggregated.itertuples():
        user_embeddings = get_dict_values_as_array(look_up_dict=id_to_embedding_lookup, key_list=i.main_recipe_id)
        user_embedings_dict[i.billing_agreement_id] = user_embeddings
    return user_embedings_dict


def pad_popular_recipe(
    df_recipes_per_user_aggregated: pd.DataFrame,
    df_order_history_train: pd.DataFrame,
    min_recipes_per_user: int = 5,
) -> pd.DataFrame:
    df_concept_dishes = get_user_concept_dishes(
        df_order_history_train=df_order_history_train,
        top_n_per_user=min_recipes_per_user,
        top_n_per_concept=(min_recipes_per_user + 3),
        look_back_weeks=24,
        df_user_preferences=None,
    )
    df_recipes_per_user_aggregated["num_recipes_from_history"] = df_recipes_per_user_aggregated["main_recipe_id"].apply(
        lambda x: len(x)
    )

    df_recipes_per_user_aggregated["num_recipes_to_pad"] = (
        min_recipes_per_user - df_recipes_per_user_aggregated["num_recipes_from_history"]
    )
    df_recipes_per_user_aggregated["num_recipes_to_pad"] = df_recipes_per_user_aggregated["num_recipes_to_pad"].apply(
        lambda x: max(0, x)
    )
    df_recipes_per_user_aggregated = df_recipes_per_user_aggregated.merge(df_concept_dishes, how="left")
    df_recipes_per_user_aggregated["main_recipe_id"] = df_recipes_per_user_aggregated.apply(
        lambda row: row["popular_concept_recipe_ids"][: row["num_recipes_to_pad"]] + row["main_recipe_id"], axis=1
    )
    df_recipes_per_user_aggregated = df_recipes_per_user_aggregated.drop(
        columns=["num_recipes_from_history", "num_recipes_to_pad"]
    )
    return df_recipes_per_user_aggregated


def pool_user_embeddings(user_embedings_dict: dict, method: str = "mean") -> dict:
    user_embedings_pooled_dict = {}

    for ba_id, embeddings in user_embedings_dict.items():
        if method == "mean":
            user_embedings_pooled_dict[ba_id] = np.mean(embeddings, axis=0)
        elif method == "max":
            user_embedings_pooled_dict[ba_id] = np.max(embeddings, axis=0)
    return user_embedings_pooled_dict
