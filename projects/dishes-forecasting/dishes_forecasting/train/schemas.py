import pandera as pa
from pandera.typing import Series


class FlexOrders(pa.DataFrameModel):
    year: Series[int] = pa.Field(in_range={"min_value": 2000, "max_value": 3000})
    week: Series[int] = pa.Field(in_range={"min_value": 1, "max_value": 53})
    company_id: Series[str] = pa.Field(str_length=36)
    variation_id: Series[str] = pa.Field(str_length=36)
    dish_ratio: Series[float] = pa.Field(ignore_na=True)
    total_dish_quantity: Series[int] = pa.Field(ignore_na=True, coerce=True)
