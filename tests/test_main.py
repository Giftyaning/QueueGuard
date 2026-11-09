import asyncio
import json
from queue import Queue

import httpx
import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def test_queue(monkeypatch):
    isolated_queue = Queue(maxsize=1)
    monkeypatch.setattr("app.main.job_queue", isolated_queue)
    return isolated_queue


@pytest.fixture
def client(test_queue):
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def job_data():
    return {
        "payment_reference": "PAY-123",
        "amount": 2500,
        "currency": "GBP",
        "order_id": "ORD-001",
    }


def test_health_check(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}



def test_create_job_returns_id_and_queued_status(client):
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


def test_create_job_rejects_negative_amount(client):
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


def test_create_job_adds_job_to_queue(client, test_queue, job_data):
    response = client.post(
        "/jobs",
        json=job_data
    )

    assert response.status_code == 201
    assert response.json()["status"] == "queued"



    assert test_queue.qsize() == 1

    queued_job = test_queue.get_nowait()

    assert queued_job["job_id"] == response.json()["job_id"]
    assert queued_job["data"] == job_data


def test_full_queue_rejects_submission_and_preserves_first_job(client, test_queue, job_data):
    first = client.post("/jobs", json=job_data)
    assert first.status_code == 201

    rejected = client.post("/jobs", json=job_data)

    assert rejected.status_code == 503
    assert rejected.json() == {"detail": "Job queue is full"}
    assert test_queue.get_nowait()["job_id"] == first.json()["job_id"]
    assert test_queue.empty()
    assert client.post("/jobs", json=job_data).status_code == 201


@pytest.mark.parametrize("field", ["payment_reference", "order_id"])
def test_identifier_length_limit(client, test_queue, job_data, field):
    job_data[field] = "x" * 129
    response = client.post("/jobs", json=job_data)
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", field]
    assert test_queue.empty()

    job_data[field] = " " + "x" * 128 + " "
    assert client.post("/jobs", json=job_data).status_code == 201
    assert test_queue.get_nowait()["data"][field] == "x" * 128


def test_job_body_byte_limit(client, test_queue, job_data):
    body = json.dumps(job_data).encode()
    body = body.ljust(4096)
    headers = {"Content-Type": "application/json"}
    assert client.post("/jobs", content=body + b" ", headers=headers).status_code == 413
    assert test_queue.empty()
    assert client.post("/jobs", content=body, headers=headers).status_code == 201


def test_streamed_job_body_is_limited_without_content_length(test_queue, job_data):
    async def chunks():
        yield json.dumps(job_data).encode().ljust(4096)
        yield b" "

    async def submit():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            return await client.post(
                "/jobs", content=chunks(), headers={"Content-Type": "application/json"}
            )

    response = asyncio.run(submit())
    assert "content-length" not in response.request.headers
    assert response.status_code == 413
    assert response.json() == {"detail": "Job payload too large"}
    assert test_queue.empty()
