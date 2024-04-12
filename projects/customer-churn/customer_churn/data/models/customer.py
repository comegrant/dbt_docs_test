from datetime import datetime

from pydantic import BaseModel


class Customer(BaseModel):
    agreement_id: int
    agreement_status: str
    agreement_creation_date: datetime
    agreement_start_date: datetime
    agreement_first_delivery_date: datetime
    source: str
    source_sales_company: str
    source_sales_department: str
    source_sales_person_id: int
    source_sales_type: str
    sign_up_code: str
    onboarding_week_number: float | None = None
    number_of_deliveries: float
    number_of_deliveries_group: str
    delivery_address_city: str
    delivery_address_municipality: str
    delivery_address_county: str
    weeks_since_last_delivery: float
    weeks_since_last_delivery_group: str
    last_delivery_date: datetime
    last_delivery_year: float
    last_delivery_week: float
    next_estimated_delivery_date: datetime
    payment_method: str
    timeblock_day: str
    timeblock_interval: str
    registration_process: str
    email_reservation: bool
    phone_reservation: bool
    door_reservation: bool
    sms_reservation: bool
    agreement_regret_weeks: float
    agreement_start_year: int
    agreement_start_week: int
    agreement_first_delivery_year: int
    agreement_first_delivery_week: int
    sign_up_payment_method: str
    subscribed_delivery_week_interval: str
