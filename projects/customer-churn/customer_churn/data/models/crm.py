from pydantic import BaseModel


class CRMSegment(BaseModel):
    agreement_id: int
    current_delivery_year: int
    current_delivery_week: int
    main_segment_name: str
    sub_segment_name: str
    planned_delivery: bool
    number_of_orders: int
    bargain_hunter: bool
    revenue: float
