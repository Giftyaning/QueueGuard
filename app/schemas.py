from typing import Annotated, Literal
from pydantic import BaseModel, Field, StringConstraints, field_validator


JobIdentifier = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)
]


class CreateJobRequest(BaseModel):
    payment_reference: JobIdentifier

    amount: int = Field(gt=0)

    currency: Literal["GBP", "USD", "EUR"]

    order_id: JobIdentifier

    @field_validator("currency", mode="before")
    @classmethod
    def normalize_currency(cls, value):
        if isinstance(value, str):
            return value.upper()
        return value
