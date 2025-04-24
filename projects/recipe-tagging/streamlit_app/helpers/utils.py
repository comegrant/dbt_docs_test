import pandas as pd

language_mapped = {
    "Norwegian": "taxonomy_ids_no",
    "Swedish": "taxonomy_ids_se",
    "Danish": "taxonomy_ids_dk",
}

img_url = "https://pimimages.azureedge.net/images/resized/"


def get_image_df(data, spark):
    img_df = spark.sql(
        f"""
        select distinct
            dr.recipe_name,
            dr.recipe_id,
            rm.recipe_photo
        from gold.dim_recipes dr
        left join silver.pim__recipe_metadata rm
            on dr.recipe_metadata_id = rm.recipe_metadata_id
        where dr.recipe_id in ({", ".join(map(str, data["recipe_id"].unique()))})
        """
    ).toPandas()

    return img_df


def get_taxonomy_df(spark):
    taxonomy_df = spark.sql(
        """
        select distinct
        taxonomy_id,
        taxonomy_name_local,
        taxonomy_type_name
        from gold.dim_taxonomies
        where taxonomy_type_name like '%seo_%'
        """
    ).toPandas()

    return taxonomy_df


def load_json_to_df(file_path: str) -> pd.DataFrame:
    json_output = pd.read_json(file_path)
    data = []
    taxonomy_keys = ["taxonomy_ids_no", "taxonomy_ids_se", "taxonomy_ids_dk"]

    for recipe in json_output["recipes"]:
        recipe_id = recipe.get("recipe_id")

        for key in taxonomy_keys:
            taxonomy_ids = recipe.get(key, {})

            if isinstance(taxonomy_ids, dict):
                for tax_id, value in taxonomy_ids.items():
                    data.append(
                        {
                            "recipe_id": recipe_id,
                            "taxonomy_key": key,
                            "taxonomy_id": tax_id,
                            "value": value,
                        }
                    )

    return pd.DataFrame(data)


def process_data(df: pd.DataFrame, language: str, spark) -> pd.DataFrame:
    df["taxonomy_id"] = df["taxonomy_id"].astype(int)
    df["recipe_id"] = df["recipe_id"].astype(int)

    df = df[df["taxonomy_key"] == language_mapped[language]]

    # taxonomies
    taxonomy_df = get_taxonomy_df(spark)
    tags_mapped = {
        "seo_cuisine": "Cuisine",
        "seo_preferences": "Preference",
        "seo_protein": "Protein",
        "seo_protein_category": "Protein Category",
        "seo_trait": "Trait",
        "seo_dish_type": "Dish",
    }

    # rename taxonomy_type_name values to tags_mapped
    taxonomy_df["taxonomy_type_name"] = taxonomy_df["taxonomy_type_name"].map(
        tags_mapped
    )

    df = df.merge(taxonomy_df, on="taxonomy_id", how="left")

    # images
    img_df = get_image_df(df, spark)
    df_with_image = df.merge(img_df, on="recipe_id", how="inner")
    df_with_image["image_url"] = img_url + df_with_image["recipe_photo"]
    df_with_image = df_with_image.dropna()

    return df_with_image[
        [
            "recipe_id",
            "recipe_name",
            "taxonomy_id",
            "value",
            "taxonomy_name_local",
            "taxonomy_type_name",
            "image_url",
        ]
    ]
