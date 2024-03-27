from datetime import datetime

from pydantic import BaseModel


class Complaints(BaseModel):
    agreement_id: int
    order_id: int
    company_id: int
    delivery_year: int
    delivery_week: int
    case_line_type: str
    case_line_amount: float
    category: str
    responsible: str
    cause: str
    comment: str | None = None
    registration_date: datetime
