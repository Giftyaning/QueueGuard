from app.schemas import CreateJobRequest
import pytest
from pydantic import ValidationError

# Positive test

def test_valid_job_request():
    request = CreateJobRequest(
        payment_reference="PAY-123",
        amount=2500,
        currency="GBP",
        order_id="ORD-001"
    )

    assert request.payment_reference == "PAY-123"
    assert request.amount == 2500
    assert request.currency == "GBP"
    assert request.order_id == "ORD-001"



# Negative test

def test_negative_amount_is_rejected():
    with pytest.raises(ValidationError):
        CreateJobRequest(
            payment_reference="PAY-123",
            amount=-500,
            currency="GBP",
            order_id="ORD-001"
        )




# White spacing test

def test_whitespace_payment_reference_is_rejected():
    with pytest.raises(ValidationError):
        CreateJobRequest(
            payment_reference="   ",
            amount=2500,
            currency="GBP",
            order_id="ORD-001"
        )

    
def test_unsupported_currency_is_rejected():
    with pytest.raises(ValidationError):
        CreateJobRequest(
            payment_reference="PAY-123",
            amount=2500,
            currency="JPY",
            order_id="ORD-001"
        )
