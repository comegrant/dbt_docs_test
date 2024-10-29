class PredictionConfig:
    def __init__(self):
        self.model_uri_mapping = {
            "AMK": {
                "has_chefs_favorite_taxonomy": "runs:/d232b763aab049538ff8aee9307a3029/pyfunc_packaged_model",
                "has_family_friendly_taxonomy": "runs:/d6798687d18f42de83662ab57ad72598/pyfunc_packaged_model",
            },
            "GL": {
                "has_chefs_favorite_taxonomy": "runs:/3a8383bb85bd4323a47a93a8bc02e096/pyfunc_packaged_model",
                "has_family_friendly_taxonomy": "runs:/e2bca2c10e12419fa0bc59e4e1965a3d/pyfunc_packaged_model",
            },
            "RT": {
                "has_chefs_favorite_taxonomy": "runs:/a5296197360545ae8ae71470dc9d08d9/pyfunc_packaged_model",
                "has_family_friendly_taxonomy": "runs:/e5f2ab645b9545bf986ecc44970c45a6/pyfunc_packaged_model",
            },
            "LMK": {
                "has_chefs_favorite_taxonomy": "runs:/e14250fd8cff4d3ab1f5ecadfb563569/pyfunc_packaged_model",
                "has_family_friendly_taxonomy": "runs:/141c2453becd4b1dbfee4a057d5e38e7/pyfunc_packaged_model",
            },
        }

    def model_uri(self, company: str, target: str) -> str:
        if company not in self.model_uri_mapping or target not in self.model_uri_mapping[company]:
            raise ValueError(f"No model uri found for company '{company}' and target '{target}'")
        return self.model_uri_mapping[company][target]

    output_columns: dict[str, str] = {
        "recipe_id": "int",
        "company_id": "str",
        "chefs_favorite_probability": "float",
        "is_chefs_favorite": "bool",
        "family_friendly_probability": "float",
        "is_family_friendly": "bool",
        "created_at": "datetime64[ns]",
    }

    target_mapped: dict[str, str] = {
        "has_chefs_favorite_taxonomy": "chefs_favorite",
        "has_family_friendly_taxonomy": "family_friendly",
    }
    prediction_threshold: float = 0.5
    weeks_in_future = 5

    input_schema = "gold"
    input_table = "fact_menus"
    output_schema = "mloutputs"
    output_table = "attribute_scoring"
