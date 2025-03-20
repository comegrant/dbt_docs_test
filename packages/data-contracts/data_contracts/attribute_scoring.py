from aligned import EventTimestamp, Float, Int32, String, feature_view

from data_contracts.sources import databricks_catalog


@feature_view(name="attribute_scoring", source=databricks_catalog.schema("mloutputs").table("attribute_scoring"))
class AttributeScoring:
    recipe_id = Int32().as_entity()
    company_id = String()
    family_friendly_probability = Float()
    chefs_favorite_probability = Float()
    created_at = EventTimestamp()
