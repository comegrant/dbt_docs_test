from pydantic import BaseModel, alias_generators, Field
from typing import Literal


class Args(BaseModel):
    company_id: Literal[
        "6a2d0b60-84d6-4830-9945-58d518d27ac2",  # LMK
        "8a613c15-35e4-471f-91cc-972f933331d7",  # AM
        "09ecd4f0-ae58-4539-8e8f-9275b1859a19",  # GL
        "5e65a955-7b1a-446c-b24f-cfe576bf52d7",  # RT
    ]
    env: Literal["dev", "test", "prod"]

    @property
    def recipe_bank_env(self) -> str:
        if self.env == "dev":
            return "DEV"
        elif self.env == "test":
            return "QA"
        elif self.env == "prod":
            return "PRD"
        else:
            raise ValueError("Invalid environment")

    @property
    def company_code(self) -> str:
        if self.company_id == "6a2d0b60-84d6-4830-9945-58d518d27ac2":
            return "LMK"
        elif self.company_id == "8a613c15-35e4-471f-91cc-972f933331d7":
            return "AM"
        elif self.company_id == "09ecd4f0-ae58-4539-8e8f-9275b1859a19":
            return "GL"
        elif self.company_id == "5e65a955-7b1a-446c-b24f-cfe576bf52d7":
            return "RT"
        else:
            raise ValueError("Invalid company id")


class ConfigModel(BaseModel):
    model_config = {
        "alias_generator": alias_generators.to_camel,
        "populate_by_name": True,
    }


class RequestCookingTime(ConfigModel):
    cooking_time_from: int = Field(alias="from")
    cooking_time_to: int = Field(alias="to")
    quantity: int
    cooking_time: str | None = Field(default=None, exclude=True)

    def model_post_init(self, *args, **kwargs):
        self.cooking_time = f"{self.cooking_time_from}_{self.cooking_time_to}"


class RequestMainIngredient(ConfigModel):
    main_ingredient_id: int
    quantity: int


class RequestPriceCategory(ConfigModel):
    price_category_id: int
    quantity: int


class RequestTaxonomy(ConfigModel):
    taxonomy_id: int
    quantity: int
    taxonomy_type_id: int
    min_average_rating: float
    main_ingredients: list[RequestMainIngredient]
    cooking_times: list[RequestCookingTime]
    price_categories: list[RequestPriceCategory]


class RequestCompany(ConfigModel):
    company_id: str
    num_recipes: int
    required_recipes: list[str]
    available_recipes: list[str]
    taxonomies: list[RequestTaxonomy]


class RequestMenu(ConfigModel):
    week: int
    year: int
    companies: list[RequestCompany]


class ReponseRecipe(ConfigModel):
    recipe_id: int
    main_ingredient_id: int
    is_constraint: bool


class ResponseMainIngredient(ConfigModel):
    main_ingredient_id: int
    wanted: int
    actual: int


class ResponsePriceCategory(ConfigModel):
    price_category_id: int
    wanted: int
    actual: int


class ResponseCookingTime(ConfigModel):
    cooking_time_from: int = Field(alias="from")
    cooking_time_to: int = Field(alias="to")
    wanted: int
    actual: int


class ResponseTaxonomy(ConfigModel):
    taxonomy_id: int
    main_ingredients: list[ResponseMainIngredient]
    price_categories: list[ResponsePriceCategory]
    cooking_times: list[ResponseCookingTime]
    average_rating: float
    wanted: int
    actual: int


class ResponseCompany(ConfigModel):
    company_id: str
    recipes: list[ReponseRecipe]
    taxonomies: list[ResponseTaxonomy]


class ResponseMenu(ConfigModel):
    week: int
    year: int
    companies: list[ResponseCompany]
    status: int
    status_msg: str
