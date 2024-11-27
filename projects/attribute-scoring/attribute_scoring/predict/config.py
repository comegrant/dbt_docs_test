class PredictionConfig:
    def __init__(self):
        self.model_uri_mapping = {
            "dev": {
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
            },
            "test": {
                "AMK": {
                    "has_chefs_favorite_taxonomy": "runs:/d99f01591e0946eca8683f455f3a8824/pyfunc_packaged_model",
                    "has_family_friendly_taxonomy": "runs:/05126f64cf6b4841b03f3a50e3c5e573/pyfunc_packaged_model",
                },
                "GL": {
                    "has_chefs_favorite_taxonomy": "runs:/a79b189d9a8c460582125975d55de0a8/pyfunc_packaged_model",
                    "has_family_friendly_taxonomy": "runs:/d60d6fceebf94df38d2456916e1aee17/pyfunc_packaged_model",
                },
                "RT": {
                    "has_chefs_favorite_taxonomy": "runs:/a26ee0388dab437298b129a04e601355/pyfunc_packaged_model",
                    "has_family_friendly_taxonomy": "runs:/31b8d372da2e4ebda26c71ca75c3e4b8/pyfunc_packaged_model",
                },
                "LMK": {
                    "has_chefs_favorite_taxonomy": "runs:/ca81e972c45b473d9122bd93f9d6be9d/pyfunc_packaged_model",
                    "has_family_friendly_taxonomy": "runs:/642867ac3b46411e87372eb248c6be99/pyfunc_packaged_model",
                },
            },
        }

    def model_uri(self, env: str, company: str, target: str) -> str:
        if env not in self.model_uri_mapping:
            raise ValueError(f"No environment '{env}' found in model URI mapping.")
        if company not in self.model_uri_mapping[env]:
            raise ValueError(f"No company '{company}' found for environment '{env}'.")
        if target not in self.model_uri_mapping[env][company]:
            raise ValueError(
                f"No model URI found for company '{company}' and target '{target}' in environment '{env}'."
            )
        return self.model_uri_mapping[env][company][target]

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
