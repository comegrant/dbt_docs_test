from pydantic import BaseModel


class ProductVariation(BaseModel):
    product_id: str
    product_type_id: str
    product_name: str
    variation_id: str
    flex_financial_variation_id: str
    portions: int
    variation_meals: int
    price: int
