import logging
from typing import List, Optional  # noqa: UP035

from data_contracts.helper import snake_to_camel
from dotenv import find_dotenv, load_dotenv
from fastapi import APIRouter, Depends, FastAPI, HTTPException
from menu_optimiser.optimization import generate_menu_companies_api
from pydantic import BaseModel, Field
from starlette.responses import JSONResponse

from analytics_api.utils.auth import validate_token

load_dotenv(find_dotenv())

app = FastAPI()
mop_router = APIRouter(dependencies=[Depends(validate_token)])

logger = logging.getLogger(__name__)


class ApiModel(BaseModel):
    class Config:
        alias_generator = snake_to_camel  # snake2camel
        populate_by_name = True


# Define a Pydantic model for the input data
class IngredientsModel(ApiModel):
    main_ingredient_id: Optional[int]
    status_id: Optional[int]


class PriceModel(ApiModel):
    price_category_id: Optional[int] = Field(alias="priceCategoryId", default=None)
    quantity: Optional[int] = None
    wanted: Optional[int] = None
    actual: Optional[int] = None

    def to_dict(self):  # noqa: ANN201
        return {
            "price_category_id": self.price_category_id,
            "quantity": self.quantity,
            "wanted": self.wanted,
            "actual": self.actual,
        }


class CookingTimeModel(ApiModel):
    time_from: Optional[int] = Field(alias="from", default=None)
    time_to: Optional[int] = Field(alias="to", default=None)
    quantity: Optional[int] = None
    wanted: Optional[int] = None
    actual: Optional[int] = None

    def to_dict(self):  # noqa: ANN201
        return {
            "time_from": self.time_from,
            "time_to": self.time_to,
            "quantity": self.quantity,
            "wanted": self.wanted,
            "actual": self.actual,
        }


class MainIngredientModel(ApiModel):
    main_ingredient_id: Optional[int] = Field(alias="mainIngredientId", default=None)
    quantity: Optional[int] = None
    wanted: Optional[int] = None
    actual: Optional[int] = None

    def to_dict(self):  # noqa: ANN201
        return {
            "main_ingredient_id": self.main_ingredient_id,
            "quantity": self.quantity,
            "wanted": self.wanted,
            "actual": self.actual,
        }


class TaxonomyModel(ApiModel):
    taxonomy_id: Optional[int] = Field(alias="taxonomyId", default=None)
    quantity: Optional[int] = None
    taxonomy_type_id: Optional[int] = Field(alias="taxonomyTypeId", default=None)
    wanted: Optional[int] = None
    actual: Optional[int] = None
    main_ingredients: Optional[List[MainIngredientModel]] = Field(alias="mainIngredients", default=None)  # noqa: UP006
    price_categories: Optional[List[PriceModel]] = Field(alias="priceCategories", default=None)  # noqa: UP006
    cooking_times: Optional[List[CookingTimeModel]] = Field(alias="cookingTimes", default=None)  # noqa: UP006
    min_average_rating: Optional[float] = Field(alias="minAverageRating", default=None)

    def to_dict(self):  # noqa: ANN201
        return {
            "taxonomy_id": self.taxonomy_id,
            "quantity": self.quantity,
            "taxonomy_type_id": self.taxonomy_type_id,
            "main_ingredients": (
                [main_ingredient.to_dict() for main_ingredient in self.main_ingredients]
                if self.main_ingredients
                else None
            ),
            "price_categories": (
                [price.to_dict() for price in self.price_categories] if self.price_categories else None
            ),
            "cooking_times": (
                [cooking_time.to_dict() for cooking_time in self.cooking_times] if self.cooking_times else None
            ),
            "min_average_rating": self.min_average_rating,
        }


class RecipesModel(ApiModel):
    recipe_id: int = Field(alias="recipeId")
    main_ingredient_id: int = Field(alias="mainIngredientId")
    is_constraint: bool = Field(alias="isConstraint")


class CompanyModel(ApiModel):
    company_id: str = Field(alias="companyId")
    num_recipes: Optional[int] = Field(alias="numRecipes", default=None)
    required_recipes: Optional[List[str]] = Field(alias="requiredRecipes", default=None)  # noqa: UP006
    available_recipes: Optional[List[str]] = Field(alias="availableRecipes", default=None)  # noqa: UP006
    taxonomies: Optional[List[TaxonomyModel]]  # noqa: UP006
    recipes: Optional[List[RecipesModel]] = None  # noqa: UP006

    def to_dict(self):  # noqa: ANN201
        return {
            "company_id": self.company_id,
            "num_recipes": self.num_recipes,
            "required_recipes": self.required_recipes,
            "available_recipes": self.available_recipes,
            "taxonomies": [taxonomy.to_dict() for taxonomy in self.taxonomies],  # type: ignore
        }


class MenuPlannerOptimizationInputModel(ApiModel):
    """
    The input model class for the menu planner.
    {
        company_id:
        year:
        week:
        rules: {}
    }
    """

    year: int
    week: int
    companies: List[CompanyModel]  # noqa: UP006

    def to_dict(self):  # noqa: ANN201
        return {
            "year": self.year,
            "week": self.week,
            "companies": [company.to_dict() for company in self.companies],
        }


class MenuPlannerOptimizationOutputModel(ApiModel):
    """
    The results output model class for menu planner.
    Contains the List of dishes
    """

    week: int
    year: int
    STATUS: int
    STATUS_MSG: str
    companies: Optional[List[CompanyModel]] = None  # noqa: UP006


@mop_router.post("/menu", response_model=MenuPlannerOptimizationOutputModel, tags=["Menu Planner"])
async def menu_generator(request: MenuPlannerOptimizationInputModel):  # noqa: ANN201
    try:
        request_dict = request.to_dict()
        logger.info("Received request for menu generation: %s", request_dict)

        response = await generate_menu_companies_api(
            week=request_dict["week"], year=request_dict["year"], companies=request_dict["companies"]
        )
        logger.info("Menu generation completed: %s", response)
        print(response)  # if we want to display results to the terminal also  # noqa: T201#
        response = MenuPlannerOptimizationOutputModel(**response)

        logger.info("Returning menu response")
        return JSONResponse(content=response.model_dump(), status_code=200)
    except Exception as e:
        logger.error("Menu generation failed", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
