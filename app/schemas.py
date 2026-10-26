from typing import Annotated, Literal
from pydantic import BaseModel, Field, StringConstraints, field_validator


class CreateJobRequest(BaseModel):
    payment_reference: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1)
    ]

    amount: int = Field(gt=0)

    currency: Literal["GBP", "USD", "EUR"]

    order_id: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1)
    ]

    @field_validator("currency", mode="before")
    @classmethod
    def normalize_currency(cls, value):
        if isinstance(value, str):
            return value.upper()
        return value