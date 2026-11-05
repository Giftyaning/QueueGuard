from fastapi.testclient import TestClient
from app.main import app
from app.job_queue import job_queue
from queue import Queue

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


def test_create_job_adds_job_to_queue(monkeypatch):

    job_data = {
        "payment_reference": "PAY-123",
        "amount": 2500,
        "currency": "GBP",
        "order_id": "ORD-001"
    }

    test_queue = Queue()

    monkeypatch.setattr(
        "app.main.job_queue",
        test_queue
    )

    response = client.post(
        "/jobs",
        json=job_data
    )

    assert response.status_code == 201
    assert response.json()["status"] == "queued"



    assert test_queue.qsize() == 1

    queued_job = test_queue.get_nowait()

    assert queued_job["job_id"] == response.json()["job_id"]
    assert queued_job["data"]["payment_reference"] == job_data["payment_reference"]
