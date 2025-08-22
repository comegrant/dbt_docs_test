class PredictionConfig:
    def __init__(self):
        pass

    def model_uri(self, env: str, company: str, target: str, alias: str) -> str:
        if not all([env, company, target]):
            raise ValueError("env, company and target must not be None")
        return f"models:/{env}.mloutputs.attribute_scoring_{company.lower()}_{target}@{alias}"

    output_columns: dict[str, type] = {
        "recipe_id": int,
        "company_id": str,
        "chefs_favorite_probability": float,
        "is_chefs_favorite": bool,
        "family_friendly_probability": float,
        "is_family_friendly": bool,
        "created_at": "datetime64[ns]",  # type: ignore
    }

    metadata_columns: list[str] = [
        "run_id",
        "run_timestamp",
        "company_id",
        "start_yyyyww",
        "end_yyyyww",
    ]

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
