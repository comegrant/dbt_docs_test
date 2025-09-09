from datetime import timedelta

from aligned import Float32, Int32, List, String, Struct, Timestamp, feature_view
from project_owners.owner import Owner
from pydantic import BaseModel

from data_contracts.sources import ml_outputs, redis_cluster
from data_contracts.tags import Tags


@feature_view(
    source=ml_outputs.table("reci_pick_recommendations"),
    contacts=[Owner.sylvia().name, Owner.matsmoll().name],
)
class Recommendations:
    billing_agreement_id = Int32().as_entity()
    menu_year = Int32().as_entity()
    menu_week = Int32().as_entity()

    company_id = String()

    main_recipe_ids = List(Int32())
    scores = List(Float32())

    model_version = String()

    created_at = Timestamp().as_freshness()


sql_query = """with runs as (
    select
        menu_year,
        menu_week,
        run_id,
        company_id,
        created_at,
        row_number() OVER(partition by menu_year, menu_week, company_id order by created_at desc) AS row_num
    from mloutputs.reci_pick_scores_metadata_menus_predicted
    where
        menu_year >= year(next_day(current_date(), 'Monday') - INTERVAL 3 DAYS)
        and menu_week >= weekofyear(current_date() + INTERVAL 1 WEEK)
),

latest_run as (
    select * from runs where row_num = 1
),

recommendations as (
    select
        company_id,
        billing_agreement_id,
        menu_year,
        menu_week,
        main_recipe_ids as main_recipe_id,
        scores as score,
        run_id,
        model_version,
        created_at
    from mloutputs.reci_pick_recommendations
),

fact_billing_agreement_consents as (
    select
        billing_agreement_id,
        is_accepted_consent,
        fk_dim_consent_types,
        valid_from
    from gold.fact_billing_agreement_consents
),

consent_types as (
    select
        pk_dim_consent_types,
        consent_category_id
    from gold.dim_consent_types
),

data_processing_consents as (
    select
        fact_billing_agreement_consents.*
    from fact_billing_agreement_consents
    left join consent_types
        on fact_billing_agreement_consents.fk_dim_consent_types = consent_types.pk_dim_consent_types
    where consent_types.consent_category_id = '3495C28B-703C-44AA-B6E0-E01D46684261' -- data processing
),

consents_with_row_number as (
    select
        *,
        row_number() over(partition by billing_agreement_id order by valid_from desc) as consent_row_number
    from data_processing_consents
),

latest_consent as (
    select
        *
    from consents_with_row_number
    where consent_row_number = 1
),

latest_recommendations as (
    select
        recommendations.*
    from recommendations
    inner join latest_run
    on recommendations.menu_year = latest_run.menu_year
        and recommendations.menu_week = latest_run.menu_week
        and recommendations.run_id = latest_run.run_id
        and recommendations.company_id = latest_run.company_id
    left join latest_consent
        on latest_consent.billing_agreement_id = recommendations.billing_agreement_id
    where latest_consent.is_accepted_consent = true
)

select
    menu_year,
    menu_week,
    company_id,
    billing_agreement_id,
    arrays_zip(main_recipe_id, score) as recipes,
    model_version
from latest_recommendations"""


class RecipeRow(BaseModel):
    main_recipe_id: int
    score: float


@feature_view(
    source=ml_outputs.config.sql(sql_query),
    materialized_source=redis_cluster,
    contacts=[Owner.sylvia().name, Owner.matsmoll().name],
    unacceptable_freshness=timedelta(days=10),
    tags=[Tags.skip_dbt_check],
)
class LatestRecommendations:
    billing_agreement_id = Int32().as_entity()
    menu_year = Int32().as_entity()
    menu_week = Int32().as_entity()

    company_id = String()

    recipes = List(Struct(RecipeRow))

    model_version = String()


default_chefs_recs = """with runs as (
    select
        menu_year,
        menu_week,
        company_id,
        created_at,
        model_version,
        main_recipe_ids as main_recipe_id,
        scores as score,
        row_number() OVER(partition by menu_year, menu_week, company_id order by created_at desc) AS row_num
    from mloutputs.reci_pick_recommendations_concept_default
    where concept_id_combinations = 'C94BCC7E-C023-40CE-81E0-C34DA3D79545'
        and menu_year >= year(next_day(current_date(), 'Monday') - INTERVAL 3 DAYS)
        and menu_week >= weekofyear(current_date() + INTERVAL 1 WEEK)
)

select company_id, menu_week, menu_year, arrays_zip(main_recipe_id, score) as recipes, model_version, created_at
from runs
where row_num = 1"""


@feature_view(
    source=ml_outputs.config.sql(default_chefs_recs),
    materialized_source=redis_cluster,
    contacts=[Owner.sylvia().name, Owner.matsmoll().name],
    unacceptable_freshness=timedelta(days=10),
    tags=[Tags.skip_dbt_check],
)
class DefaultRecommendations:
    company_id = String().as_entity()

    menu_year = Int32().as_entity()
    menu_week = Int32().as_entity()

    recipes = List(Struct(RecipeRow))

    model_version = String()
