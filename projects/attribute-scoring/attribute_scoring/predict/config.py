class PredictionConfig:
    def __init__(self):
        self.model_uri_mapping = {
            "AMK": {
                "has_chefs_favorite_taxonomy": "runs:/122295a85df94997b50d9e8951001266/pyfunc_packaged_model",
                "has_family_friendly_taxonomy": "runs:/89b200e07d18434598fe9962d0dcd250/pyfunc_packaged_model",
            },
            "GL": {
                "has_chefs_favorite_taxonomy": "runs:/f2b9b995c68347d0825294b17b147daa/pyfunc_packaged_model",
                "has_family_friendly_taxonomy": "runs:/67b82d6c2c044db2b835810a030b238a/pyfunc_packaged_model",
            },
            "RT": {
                "has_chefs_favorite_taxonomy": "runs:/c88c2e757d764ba2a42accfb59982dec/pyfunc_packaged_model",
                "has_family_friendly_taxonomy": "runs:/401f5f7f14ec4189b1b06d1d26c9a7eb/pyfunc_packaged_model",
            },
            "LMK": {
                "has_chefs_favorite_taxonomy": "runs:/0ec7191654104516a88797af706544ae/pyfunc_packaged_model",
                "has_family_friendly_taxonomy": "runs:/81de530990f54b0ca8f7d4b5395e5cf7/pyfunc_packaged_model",
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
