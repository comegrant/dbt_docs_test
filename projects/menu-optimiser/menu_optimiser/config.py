"""
Configuration settings for the menu optimiser.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class PathConfig:
    recipe_bank_directory: str = "data-science/test-folder/MP20"
    recipe_tagging_directory: str = "data-science/recipe_tagging/prod"
    recipe_main_ingredients_directory: str = (
        "data-science/menu_generator/dev"  # TODO: prod?
    )


class RecipeConfig:
    ingredient_columns_to_normalize: list[str] = [
        "main_group",
        "category_group",
        "product_group",
    ]

    carb_remove_groups: list[str] = ["basis", "vegetarian", "sauces", "seafood"]
    protein_remove_groups: list[str] = [
        "spices",
        "baked_goods",
        "basis",
        "sauces",
        "dry_goods",
    ]
    carb_category_groups: list[str] = [
        "soft_bread",
        "groceries",
        "hard_bread",
        "flat_bread",
        "dough",
    ]
    protein_main_groups: list[str] = ["dairy", "prepared_meals"]
    protein_product_groups: list[str] = ["preserved_greens"]
    recipe_bank_columns: list[str] = [
        "recipe_id",
        "main_ingredient_id",
        "taxonomy_id",
        "average_rating",
        "price_category",
        "is_universe",
        "cooking_time_from",
        "cooking_time_to",
    ]
    aggregation_config: dict[str, tuple[str, str | type]] = {
        "taxonomies": ("taxonomy_id", list),
        "main_ingredient_id": ("main_ingredient_id", "first"),
        "price_category_id": ("price_category", "first"),
        "average_rating": ("average_rating", "first"),
        "is_universe": ("is_universe", "first"),
        "cooking_time_from": ("cooking_time_from", "first"),
        "cooking_time_to": ("cooking_time_to", "first"),
        "cooking_time": ("cooking_time", "first"),
    }

    fill_columns: dict[str, list[int | str]] = {
        "dish_type": [-1],
        "cuisine": [-1],
        "main_carb": ["unknown"],
        "main_protein": ["unknown"],
    }


@dataclass(frozen=True)
class OptimizationConfig:
    """Configuration for a specific company."""

    diversity_config: dict[str, float]
    max_cold_storage: int
    max_dry_storage: int
    amount_of_recipes: int | None = None
    taxonomy_maximum: dict[int, int] | None = None


class CompanyOptimizationConfig:
    _company_configs: dict[str, OptimizationConfig] = {
        "6a2d0b60-84d6-4830-9945-58d518d27ac2": OptimizationConfig(  # LMK
            diversity_config={
                "main_protein": 5.0,
                "main_carb": 4.0,
                "protein_processing": 3.0,
                "cuisine": 2.0,
                "dish_type": 2.0,
            },
            max_cold_storage=126,
            max_dry_storage=156,
            amount_of_recipes=None,
            taxonomy_maximum=None,
        ),
        "8a613c15-35e4-471f-91cc-972f933331d7": OptimizationConfig(  # AM
            diversity_config={
                "main_protein": 5.0,
                "main_carb": 4.0,
                "protein_processing": 3.0,
                "cuisine": 2.0,
                "dish_type": 2.0,
            },
            max_cold_storage=110,  # 158, 110
            max_dry_storage=100,  # 148, 100
            amount_of_recipes=None,
            taxonomy_maximum=None,
        ),
        "09ecd4f0-ae58-4539-8e8f-9275b1859a19": OptimizationConfig(  # GL
            diversity_config={
                "main_protein": 5.0,
                "main_carb": 4.0,
                "protein_processing": 3.0,
                "cuisine": 2.0,
                "dish_type": 2.0,
            },
            max_cold_storage=110,  # 158
            max_dry_storage=100,  # 148
            amount_of_recipes=None,
            taxonomy_maximum={
                2015: 9,  # ROEDE
            },
        ),
        "5e65a955-7b1a-446c-b24f-cfe576bf52d7": OptimizationConfig(  # RT
            diversity_config={
                "main_protein": 5.0,
                "main_carb": 4.0,
                "protein_processing": 3.0,
                "cuisine": 2.0,
                "dish_type": 2.0,
            },
            max_cold_storage=172,
            max_dry_storage=108,
            amount_of_recipes=None,
            taxonomy_maximum=None,
        ),
    }

    @classmethod
    def get_company_config(cls, company_id: str) -> OptimizationConfig:
        """Get configuration for a company."""
        if company_id in cls._company_configs:
            return cls._company_configs[company_id]
        else:
            raise ValueError(f"No configuration found for company_id: {company_id}")

    print_msg: bool = False


# ----------------------------
# TAXONOMIES
# ----------------------------
dish_taxonomies = [
    3335,
    3472,
    3609,
    3339,
    3476,
    3613,
    3337,
    3474,
    3611,
    3334,
    3471,
    3608,
    3336,
    3473,
    3610,
    3338,
    3475,
    3612,
]

protein_filet = [
    3278,
    3415,
    3552,
]

protein_ground_meat = [
    3277,
    3414,
    3551,
    3280,
    3417,
    3554,
]

protein_mixed_meat = [
    3274,
    3411,
    3548,
]


cuisine_taxonomies = [
    3203,
    3340,
    3477,
    3204,
    3341,
    3478,
    3205,
    3342,
    3479,
    3206,
    3343,
    3480,
    3208,
    3345,
    3482,
    3216,
    3353,
    3490,
    3207,
    3344,
    3481,
    3209,
    3346,
    3483,
    3210,
    3347,
    3484,
    3230,
    3367,
    3504,
    3211,
    3348,
    3485,
    3212,
    3349,
    3486,
    3213,
    3350,
    3487,
    3214,
    3351,
    3488,
    3215,
    3352,
    3489,
    3217,
    3354,
    3491,
    3218,
    3355,
    3492,
    3219,
    3356,
    3493,
    3221,
    3358,
    3495,
    3220,
    3357,
    3494,
    3229,
    3366,
    3503,
    3222,
    3359,
    3496,
    3225,
    3362,
    3499,
    3223,
    3360,
    3497,
    3224,
    3361,
    3498,
    3228,
    3365,
    3502,
    3226,
    3363,
    3500,
    3227,
    3364,
    3501,
]

seasonal_taxonomies = [
    2203,
    2204,
    2205,
    2206,
    2207,
    2208,
    2209,
    2210,
    2211,
    2212,
    2213,
    2214,
    2215,
    2216,
    2217,
    2218,
    2219,
    2220,
    2221,
    2222,
    2223,
]
