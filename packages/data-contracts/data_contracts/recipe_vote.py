from aligned import (
    Int32,
    feature_view,
)

from data_contracts.sources import databricks_config, materialized_data

recipe_vote_sql = """
with raw_data as (
select
favorites.agreement_id
,favorites.recipe_id
,favorites.main_recipe_id
, case when types.recipe_favorite_type_name = "favorite" then 1 else 0 end as is_favorite
, case when types.recipe_favorite_type_name = "dislike" then 1 else 0 end as is_dislike
from bronze.pim__recipe_favorites favorites
left join bronze.pim__recipe_favorite_types types on favorites.recipe_favorite_type_id = types.recipe_favorite_type_id
)
select agreement_id, main_recipe_id, max(is_favorite) as is_favorite, max(is_dislike) as is_dislike
from raw_data
group by agreement_id, main_recipe_id;
"""


@feature_view(
    name="recipe_vote",
    source=databricks_config.sql(recipe_vote_sql),
    materialized_source=materialized_data.parquet_at(
        "recipe_vote.parquet",
    ),
)
class RecipeVote:
    agreement_id = Int32().as_entity()
    main_recipe_id = Int32().as_entity()
    is_favorite = Int32()
    is_dislike = Int32()
