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

# Currency test
    
def test_unsupported_currency_is_rejected():
    with pytest.raises(ValidationError):
        CreateJobRequest(
            payment_reference="PAY-123",
            amount=2500,
            currency="JPY",
            order_id="ORD-001"
        )

# Zero amount test

def test_zero_amount_is_rejected():
    with pytest.raises(ValidationError):
        CreateJobRequest(
            payment_reference="PAY-123",
            amount=0,
            currency="GBP",
            order_id="ORD-001"
        )

# Currency convertion test

@pytest.mark.parametrize(
    "input_currency, expected_currency",
    [
        ("gbp", "GBP"),
        ("usd", "USD"),
        ("eur", "EUR"),
    ]
)
def test_currency_normalization(
    input_currency, expected_currency
):
    request = CreateJobRequest(
        payment_reference="PAY-123",
        amount=2500,
        currency=input_currency,
        order_id="ORD-001"
    )

    assert request.currency == expected_currency