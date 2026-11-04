from fastapi.testclient import TestClient
from app.main import app
from app.job_queue import job_queue

client = TestClient(app)


def test_health_check():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}



def test_create_job_returns_id_and_queued_status():
    response = client.post(
        "/jobs",
        json={
            "payment_reference": "PAY-123",
            "amount": 2500,
            "currency": "GBP",
            "order_id": "ORD-001"
        }
    )

    assert response.status_code == 201

    data = response.json()

    assert "job_id" in data
    assert data["status"] == "queued"


def test_create_job_rejects_negative_amount():
    response = client.post(
        "/jobs",
        json={
            "payment_reference": "PAY-123",
            "amount": -500,
            "currency": "GBP",
            "order_id": "ORD-001"
        }
    )

    assert response.status_code == 422


def test_create_job_adds_job_to_queue():

    pass