from aligned import EventTimestamp, Float32, Int32, String, feature_view

from data_contracts.sources import databricks_catalog, materialized_data


@feature_view(
    name="attribute_scoring",
    source=databricks_catalog.schema("mloutputs").table("attribute_scoring"),
    materialized_source=materialized_data.partitioned_parquet_at("attribute_scoring", partition_keys=["company_id"]),
)
class AttributeScoring:
    recipe_id = Int32().as_entity()
    company_id = String()
    family_friendly_probability = Float32()
    chefs_favorite_probability = Float32()
    created_at = EventTimestamp()
