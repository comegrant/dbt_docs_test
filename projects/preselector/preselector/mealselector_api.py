import logging

import pandas as pd
from pydantic import BaseModel

from preselector.data.models.customer import PreselectorCustomer

logger = logging.getLogger(__name__)


class MealSelectorRequest(BaseModel):
    weeks: list[dict[str, int]]
    companyId: str
    agreementId: int
    products: list[dict[str, str]]
    preferences: list[dict[str, str]]
    price: int


class MealSelectorProductResponse(BaseModel):
    variationId: str
    quantity: int


class MealSelectorWeekResponse(BaseModel):
    week: int
    year: int
    result: str
    products: list[MealSelectorProductResponse]


class MealSelectorReponse(BaseModel):
    agreementId: int
    weeks: list[MealSelectorWeekResponse]


async def analytics_api_token() -> str | Exception:
    import os

    from httpx import AsyncClient

    url = "https://analytics.godtlevert.no/token"

    async with AsyncClient() as client:
        username = os.environ.get("API_USERNAME")
        password = os.environ.get("API_PASSWORD")

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        response = await client.post(
            url,
            data={"username": username, "password": password},
            headers=headers,
        )

        try:
            response.raise_for_status()
        except Exception as e:
            return e

        json_resp = response.json()
        token = json_resp["access_token"]

    return token


async def run_mealselector(
    customer: PreselectorCustomer,
    year: int,
    week: int,
    menu: pd.DataFrame,
) -> list[int] | Exception:
    from httpx import AsyncClient

    url = "https://analytics.godtlevert.no/Preferences/GeneratePersonalizedDeviation"
    product_variation_id = customer.subscribed_product_variation_id

    if not product_variation_id:
        return ValueError("No product variation id found")

    body = MealSelectorRequest(
        weeks=[{"week": week, "year": year}],
        agreementId=customer.agreement_id,
        companyId=customer.company_id,
        products=[{"variationId": product_variation_id.lower(), "quantity": "1"}],
        preferences=[
            {
                "preferenceId": pref.lower(),
                "preferenceTypeId": "4C679266-7DC0-4A8E-B72D-E9BB8DADC7EB".lower(),
                "preferencePriority": "0",
            }
            for pref in customer.taste_preference_ids
        ],
        price=0,
    )

    token = await analytics_api_token()
    if isinstance(token, Exception):
        return token

    async with AsyncClient() as client:
        headers = {"Authorization": f"Bearer {token}"}
        response = await client.post(
            url,
            content=body.model_dump_json(),
            headers=headers,
        )

        try:
            response.raise_for_status()
            body = MealSelectorReponse(**response.json())
        except Exception as e:
            return e

    week_response = body.weeks[0]

    if week_response.result == "ERROR":
        return ValueError("An error occurred when computing the default mealbox")

    if week_response.result == "DEFAULT":
        default_mealbox = menu[menu["variation_id"] == product_variation_id]
        default_mealbox = default_mealbox[
            default_mealbox["menu_recipe_order"] <= customer.number_of_recipes
        ]
        recipes = default_mealbox["main_recipe_id"].tolist()
    else:
        variation_ids = [x.variationId for x in week_response.products]
        selected_mealbox = menu[menu["variation_id"].isin(variation_ids)]
        recipes = selected_mealbox["main_recipe_id"].tolist()

    return recipes
