from pydantic import BaseModel


class DishVariation(BaseModel):
    name: str
    recipe_ids: list[int]
    product_type: str
    basket_size: int
    taste: str
    variation_portions: int
    taxonomies: str | None
    price: float


class Order(BaseModel):
    year: int
    week: int
    dishes: list[DishVariation]


class PlannedOrder(Order):
    is_active: bool


class DeliveredOrder(Order):
    rating: int
