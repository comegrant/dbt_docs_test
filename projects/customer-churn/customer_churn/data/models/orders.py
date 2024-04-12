from datetime import datetime

from pydantic import BaseModel


class OrderRecord(BaseModel):
    agreement_id: int
    company_name: str
    order_id: float
    delivery_date: datetime
    delivery_year: int
    delivery_week: int
    net_revenue_ex_vat: float
    gross_revenue_ex_vat: float
