from datetime import datetime

from pydantic import BaseModel


class EventRecord(BaseModel):
    agreement_id: int
    event_text: str
    timestamp: datetime
