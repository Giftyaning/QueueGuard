from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Event, Lock

from fastapi.testclient import TestClient
import pytest

from app.job_queue import get_status_retention_seconds
from app.main import create_app
from app.worker import JobWorker


class Clock:
    def __init__(self):
        self._now = 0.0
        self._lock = Lock()

    def __call__(self):
        with self._lock:
            return self._now

    def advance(self, seconds):
        with self._lock:
            self._now += seconds


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def data():
    return {
        "payment_reference": "PAY-123",
        "amount": 2500,
        "currency": "GBP",
        "order_id": "ORD-001",
    }


def wait_for_tasks(worker):
    with worker.queue.all_tasks_done:
        assert worker.queue.all_tasks_done.wait_for(
            lambda: worker.queue.unfinished_tasks == 0, timeout=2
        )


@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize("expiry_time", [10, 11])
def test_terminal_status_expiry_and_defensive_copy(clock, data, failed, expiry_time):
    def processor(data, stop):
        if failed:
            raise ValueError("controlled failure")

    app = create_app(processor=processor, status_retention_seconds=10, clock=clock)
    worker = app.state.worker
    with TestClient(app) as client:
        job_id = client.post("/jobs", json=data).json()["job_id"]
        wait_for_tasks(worker)
        clock.advance(9)
        expected = {
            "job_id": job_id, "status": "failed" if failed else "completed", "data": data
        }
        assert client.get(f"/jobs/{job_id}").json() == expected
        snapshot = worker.get_job(job_id)
        snapshot["data"]["amount"] = -1
        assert worker.get_job(job_id) == expected
        clock.advance(expiry_time - 9)
        response = client.get(f"/jobs/{job_id}")
        assert response.status_code == 404
        assert response.json() == {"detail": "Job not found"}
        assert job_id not in worker._jobs
        assert job_id not in worker._expires_at
    assert not worker.thread.is_alive()


@pytest.mark.parametrize("failed", [False, True])
def test_retention_starts_at_completion_and_active_jobs_do_not_expire(clock, data, failed):
    entered = Event()
    release = Event()

    def processor(data, stop):
        entered.set()
        release.wait()
        if failed:
            raise ValueError("controlled failure")

    app = create_app(
        processor=processor, queue_max_size=1, status_retention_seconds=10, clock=clock
    )
    worker = app.state.worker
    with TestClient(app) as client:
        try:
            first = client.post("/jobs", json=data).json()["job_id"]
            assert entered.wait(2)
            second = client.post("/jobs", json=data).json()["job_id"]
            clock.advance(100)
            worker.cleanup_expired()
            assert client.get(f"/jobs/{first}").json()["status"] == "processing"
            assert client.get(f"/jobs/{second}").json()["status"] == "queued"
            release.set()
            wait_for_tasks(worker)
            clock.advance(9)
            for job_id in (first, second):
                assert client.get(f"/jobs/{job_id}").json()["status"] == (
                    "failed" if failed else "completed"
                )
            clock.advance(1)
            for job_id in (first, second):
                assert client.get(f"/jobs/{job_id}").status_code == 404
        finally:
            release.set()


def test_lookup_rejects_expiry_before_scheduled_cleanup(clock, data):
    entered = Event()
    release = Event()

    def processor(data, stop):
        entered.set()
        release.wait()

    app = create_app(processor=processor, status_retention_seconds=10, clock=clock)
    worker = app.state.worker
    with TestClient(app) as client:
        try:
            job_id = client.post("/jobs", json=data).json()["job_id"]
            assert entered.wait(2)
            clock.advance(9)
            release.set()
            wait_for_tasks(worker)
            clock.advance(1)
            worker.cleanup_expired()  # Next cleanup is at 20; job expires at 19.
            clock.advance(9)
            assert clock() < worker._next_cleanup
            assert job_id in worker._jobs
            assert client.get(f"/jobs/{job_id}").status_code == 404
            assert job_id not in worker._expires_at
        finally:
            release.set()


def test_idle_worker_periodically_removes_records_without_lookup(clock, data):
    cleaned = Event()

    class ObservedWorker(JobWorker):
        def cleanup_expired(self):
            super().cleanup_expired()
            with self._lock:
                if clock() >= 10 and not self._jobs:
                    cleaned.set()

    worker = ObservedWorker(Queue(maxsize=1), status_retention_seconds=10, clock=clock)
    worker.submit(data)
    worker.start()
    try:
        wait_for_tasks(worker)
        clock.advance(10)
        # The real idle loop schedules cleanup. No lookup/manual cleanup triggers it.
        assert cleaned.wait(2)
        with worker._lock:
            assert not worker._jobs
            assert not worker._expires_at
    finally:
        worker.stop()
    assert not worker.thread.is_alive()


def test_concurrent_submission_lookup_and_cleanup(clock, data):
    worker = JobWorker(Queue(maxsize=50), status_retention_seconds=10, clock=clock)
    old_ids = [worker.submit(data)["job_id"] for _ in range(10)]
    worker.start()
    try:
        wait_for_tasks(worker)
        clock.advance(10)
        start = Event()

        def lookups():
            assert start.wait(2)
            for job_id in old_ids:
                assert worker.get_job(job_id) is None

        def cleanup():
            assert start.wait(2)
            for _ in range(20):
                worker.cleanup_expired()

        def submit():
            assert start.wait(2)
            return [worker.submit(data)["job_id"] for _ in range(20)]

        with ThreadPoolExecutor(max_workers=3) as pool:
            lookup_task = pool.submit(lookups)
            cleanup_task = pool.submit(cleanup)
            submit_task = pool.submit(submit)
            start.set()
            lookup_task.result(timeout=2)
            cleanup_task.result(timeout=2)
            new_ids = submit_task.result(timeout=2)
        wait_for_tasks(worker)
        for job_id in new_ids:
            assert worker.get_job(job_id)["status"] == "completed"
        with worker._lock:
            assert set(worker._jobs) == set(new_ids)
            assert set(worker._expires_at) == set(new_ids)
    finally:
        worker.stop()
    assert not worker.thread.is_alive()


def test_applications_have_independent_retention_state(data):
    first_clock, second_clock = Clock(), Clock()
    first = create_app(status_retention_seconds=10, clock=first_clock)
    second = create_app(status_retention_seconds=20, clock=second_clock)
    with TestClient(first) as first_client, TestClient(second) as second_client:
        first_id = first_client.post("/jobs", json=data).json()["job_id"]
        second_id = second_client.post("/jobs", json=data).json()["job_id"]
        wait_for_tasks(first.state.worker)
        wait_for_tasks(second.state.worker)
        first_clock.advance(10)
        first.state.worker.cleanup_expired()
        assert first_client.get(f"/jobs/{first_id}").status_code == 404
        assert second_client.get(f"/jobs/{second_id}").json()["status"] == "completed"
        assert second_client.get(f"/jobs/{first_id}").status_code == 404
        second_clock.advance(20)
        assert second_client.get(f"/jobs/{second_id}").status_code == 404
    for app in (first, second):
        assert not app.state.worker.thread.is_alive()


def test_retention_configuration_default_and_environment(monkeypatch):
    monkeypatch.delenv("QUEUEGUARD_STATUS_RETENTION_SECONDS", raising=False)
    assert get_status_retention_seconds() == 3600
    assert create_app().state.worker.status_retention_seconds == 3600
    monkeypatch.setenv("QUEUEGUARD_STATUS_RETENTION_SECONDS", "12.5")
    assert create_app().state.worker.status_retention_seconds == 12.5
    assert create_app(status_retention_seconds=7).state.worker.status_retention_seconds == 7


@pytest.mark.parametrize("value", ["", "abc", "0", "-1", "nan", "inf", "-inf", "1e999"])
def test_invalid_environment_rejected_at_app_creation(monkeypatch, value):
    monkeypatch.setenv("QUEUEGUARD_STATUS_RETENTION_SECONDS", value)
    with pytest.raises(ValueError, match="QUEUEGUARD_STATUS_RETENTION_SECONDS.*positive and finite"):
        create_app()


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), True])
def test_invalid_injected_retention_is_rejected(value):
    with pytest.raises(ValueError, match="positive and finite"):
        create_app(status_retention_seconds=value)
