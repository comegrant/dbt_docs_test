import pandera as pa
from pandera.typing import Series


class OrderEstimationFeatures(pa.DataFrameModel):
    year: Series[pa.dtypes.Int32] = pa.Field(
        in_range={"min_value": 2000, "max_value": 3000}
    )
    week: Series[pa.dtypes.Int32] = pa.Field(in_range={"min_value": 1, "max_value": 53})
    company_id: Series[str] = pa.Field(str_length=36)
    estimation_date: Series[pa.dtypes.Timestamp]
    quantity_estimated_total: Series[pa.dtypes.Int32]
    quantity_estimated_dishes: Series[float]
    cut_off_date: Series[pa.dtypes.Timestamp]
    num_days_to_cut_off: Series[pa.dtypes.Int32]


class OrderHistoryFeatures(pa.DataFrameModel):
    year: Series[pa.dtypes.Int32] = pa.Field(
        in_range={"min_value": 2000, "max_value": 3000}
    )
    week: Series[pa.dtypes.Int32] = pa.Field(in_range={"min_value": 1, "max_value": 53})
    company_id: Series[str] = pa.Field(str_length=36)
    first_date_of_week: Series[pa.dtypes.Timestamp]
    seasonality: Series[float]


class OrderEstimationHistoryRetentionFeatures(pa.DataFrameModel):
    year: Series[pa.dtypes.Int32] = pa.Field(
        in_range={"min_value": 2000, "max_value": 3000}
    )
    week: Series[pa.dtypes.Int32] = pa.Field(in_range={"min_value": 1, "max_value": 53})
    company_id: Series[str] = pa.Field(str_length=36)
    estimation_date: Series[pa.dtypes.Timestamp]


class CountryHolidaysFeatures(pa.DataFrameModel):
    year: Series[pa.dtypes.Int32] = pa.Field(
        in_range={"min_value": 2000, "max_value": 3000}
    )
    week: Series[pa.dtypes.Int32] = pa.Field(in_range={"min_value": 1, "max_value": 53})
    country: Series[str] = pa.Field(str_length=15)
    # Holiday features below
