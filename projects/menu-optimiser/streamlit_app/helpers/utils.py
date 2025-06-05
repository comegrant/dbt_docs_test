import pandas as pd
from catalog_connector import connection
from menu_optimiser.data import get_recipe_bank_data, preprocess_recipe_data
from menu_optimiser.common import Args
import plotly.graph_objects as go
import seaborn as sns
from typing import Union

URLS = {
    "img_url": "https://pimimages.azureedge.net/images/resized/",
    "pim_url": "https://pim.cheffelo.com/recipes/edit/",
}


# need to
async def get_recipe_data(recipes: list[int], company_id: str) -> pd.DataFrame:
    args = Args(company_id=company_id, env="prod")

    df = connection.sql(
        """
    select
        dr.recipe_id,
        dr.recipe_name,
        dr.recipe_main_ingredient_name_local,
        dr.is_in_recipe_universe,
        dr.cooking_time_from,
        dr.cooking_time_to,
        dr.cooking_time,
        rm.recipe_photo
    from gold.dim_recipes dr
    left join silver.pim__recipe_metadata rm
        on dr.recipe_metadata_id = rm.recipe_metadata_id
    where recipe_id in ({})
    """.format(",".join(map(str, recipes)))
    ).toPandas()

    df["image_link"] = URLS["img_url"] + df["recipe_photo"].astype(str)
    df["pim_link"] = URLS["pim_url"] + df["recipe_id"].astype(str)

    recipe_bank_data = await get_recipe_bank_data(
        args.company_code, args.recipe_bank_env
    )
    recipe_bank_df = pd.DataFrame(
        preprocess_recipe_data(recipe_bank_data, only_universe=True)
    )

    final = pd.merge(
        df,
        recipe_bank_df[
            ["recipe_id", "taxonomies", "price_category_id", "average_rating"]
        ],
        on="recipe_id",
        how="left",
    )

    return final


def map_taxonomies(data: pd.DataFrame, relevant_taxonomies: list[int]) -> pd.DataFrame:
    taxonomies = connection.sql(
        """
        select
            taxonomy_id,
            taxonomy_name_local
        from gold.dim_taxonomies
        where taxonomy_id in ({})
        """.format(",".join(map(str, relevant_taxonomies)))
    ).toPandas()

    exploded = data.explode("taxonomies").rename(columns={"taxonomies": "taxonomy_id"})
    # only keep relevant taxonomies
    exploded = exploded[exploded["taxonomy_id"].isin(relevant_taxonomies)]

    merged = pd.merge(exploded, taxonomies, on="taxonomy_id", how="left")

    taxonomy_names = merged.groupby("recipe_id")["taxonomy_name_local"].agg(list)

    return pd.merge(data, taxonomy_names, on="recipe_id", how="left")


def plot_donut(series, title):
    counts = series.value_counts()
    labels = counts.index.tolist()
    values = counts.values.tolist()

    # color palette
    colors = sns.color_palette("rocket", n_colors=len(labels)).as_hex()  # rocket

    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                hole=0.5,
                hoverinfo="label+percent+value",
                textinfo="label+percent",
                marker=dict(colors=colors, line=dict(color="#000", width=1)),
            )
        ]
    )
    fig.update_layout(
        title_text=title, showlegend=True, height=400, margin=dict(t=50, b=50, l=0, r=0)
    )
    return fig


def format_as_tags(preferences: Union[str, list[str], None], color: str) -> str:
    if preferences is None:
        return ""

    # Handle case where preferences might be a float or other non-iterable
    if not isinstance(preferences, (str, list)):
        return ""

    pref_list: list[str] = (
        [preferences] if isinstance(preferences, str) else list(preferences)
    )
    tags: str = " ".join(
        [
            f"<span style='background-color:{color}; "
            f"color:white; padding:2px 4px; margin:1px; "
            f"border-radius:4px; font-size:0.9em;'>"
            f"{pref}</span>"
            for pref in pref_list
        ]
    )
    return tags
