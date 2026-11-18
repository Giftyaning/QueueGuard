from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Event
from unittest.mock import Mock

from fastapi.testclient import TestClient
import pytest

from app.main import create_app
from app.worker import JobWorker, process_job


@pytest.fixture
def job_data():
    return {
        "payment_reference": "PAY-123",
        "amount": 2500,
        "currency": "GBP",
        "order_id": "ORD-001",
    }


def wait_for_tasks(queue):
    # Queue.join() has no timeout. Use its condition to bound failures in tests.
    with queue.all_tasks_done:
        assert queue.all_tasks_done.wait_for(lambda: queue.unfinished_tasks == 0, timeout=2)


def test_lookup_queued_and_unknown_jobs(job_data):
    app = create_app(queue_max_size=1, start_worker=False)
    with TestClient(app) as client:
        accepted = client.post("/jobs", json=job_data)
        assert accepted.status_code == 201
        job = accepted.json()
        assert client.get(f"/jobs/{job['job_id']}").json() == {**job, "data": job_data}
        response = client.get("/jobs/unknown")
        assert response.status_code == 404
        assert response.json() == {"detail": "Job not found"}


def test_lifecycle_and_waiting_capacity(job_data):
    entered = Event()
    release = Event()

    def processor(data, stop):
        entered.set()
        release.wait()

    app = create_app(queue_max_size=1, processor=processor)
    queue = app.state.worker.queue
    with TestClient(app) as client:
        try:
            first = client.post("/jobs", json=job_data).json()
            assert entered.wait(2)
            assert client.get(f"/jobs/{first['job_id']}").json()["status"] == "processing"
            # The in-flight job no longer occupies a waiting slot.
            second_response = client.post("/jobs", json=job_data)
            assert second_response.status_code == 201
            second = second_response.json()
            assert client.get(f"/jobs/{second['job_id']}").json()["status"] == "queued"
            assert client.post("/jobs", json=job_data).status_code == 503
            assert len(app.state.worker._jobs) == 2
            release.set()
            wait_for_tasks(queue)
            for job in (first, second):
                assert client.get(f"/jobs/{job['job_id']}").json() == {
                    "job_id": job["job_id"], "status": "completed", "data": job_data
                }
        finally:
            release.set()
    assert not app.state.worker.thread.is_alive()


def test_failure_is_private_and_worker_continues(job_data, caplog):
    calls = 0

    def processor(data, stop):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError(data["payment_reference"])

    app = create_app(queue_max_size=2, processor=processor)
    queue = app.state.worker.queue
    with TestClient(app) as client:
        failed = client.post("/jobs", json=job_data).json()
        completed = client.post("/jobs", json=job_data).json()
        wait_for_tasks(queue)
        failure_response = client.get(f"/jobs/{failed['job_id']}")
        assert failure_response.json()["status"] == "failed"
        assert set(failure_response.json()) == {"job_id", "status", "data"}
        assert client.get(f"/jobs/{completed['job_id']}").json()["status"] == "completed"
        assert queue.unfinished_tasks == 0
    assert not app.state.worker.thread.is_alive()
    assert failed["job_id"] in caplog.text
    assert "ValueError" in caplog.text
    assert job_data["payment_reference"] not in caplog.text


def test_idle_worker_stops():
    app = create_app(queue_max_size=1)
    with TestClient(app):
        assert app.state.worker.thread.is_alive()
    assert not app.state.worker.thread.is_alive()


def test_shutdown_signals_active_processor(job_data):
    entered = Event()
    cancelled = Event()

    def processor(data, stop):
        entered.set()
        # Lifespan signals this event even if an assertion inside the client fails.
        stop.wait()
        cancelled.set()
        raise InterruptedError("cancelled")

    app = create_app(queue_max_size=1, processor=processor)
    queue = app.state.worker.queue
    with TestClient(app) as client:
        job = client.post("/jobs", json=job_data).json()
        assert entered.wait(2)
    assert cancelled.is_set()
    assert not app.state.worker.thread.is_alive()
    assert app.state.worker.get_job(job["job_id"])["status"] == "failed"
    assert queue.unfinished_tasks == 0


def test_concurrent_submissions_have_consistent_records(job_data):
    queue = Queue(maxsize=20)
    worker = JobWorker(queue)
    worker.start()
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            accepted = list(pool.map(lambda _: worker.submit(job_data), range(20)))
        wait_for_tasks(queue)
        assert len({job["job_id"] for job in accepted}) == 20
        for job in accepted:
            assert worker.get_job(job["job_id"]) == {
                **job, "status": "completed", "data": job_data
            }
        snapshot = worker.get_job(accepted[0]["job_id"])
        snapshot["data"]["amount"] = -1
        assert worker.get_job(accepted[0]["job_id"])["data"] == job_data
    finally:
        worker.stop()
    assert not worker.thread.is_alive()


def test_placeholder_has_no_data_side_effects(job_data):
    original = job_data.copy()
    assert process_job(job_data, Event()) is None
    assert job_data == original
    stop = Event()
    stop.set()
    with pytest.raises(InterruptedError):
        process_job(job_data, stop)


def test_shutdown_wait_is_bounded_and_can_be_finished(job_data):
    entered = Event()
    release = Event()

    def processor(data, stop):
        entered.set()
        release.wait()

    worker = JobWorker(Queue(maxsize=1), processor)
    job = worker.submit(job_data)
    worker.start()
    try:
        assert entered.wait(2)
        with pytest.raises(RuntimeError, match="shutdown timeout"):
            worker.stop(timeout=0.01)
        assert worker.stop_requested.is_set()
        assert worker.thread.is_alive()
        assert worker.get_job(job["job_id"])["status"] == "processing"
        with pytest.raises(RuntimeError, match="stopping"):
            worker.submit(job_data)
    finally:
        release.set()
        worker.stop()
    assert not worker.thread.is_alive()


def test_stop_passes_default_timeout_to_join():
    worker = JobWorker(Queue(maxsize=1))
    worker.thread = Mock()
    worker.thread.is_alive.return_value = False
    worker.stop()
    worker.thread.join.assert_called_once_with(timeout=2.0)
    assert worker.stop_requested.is_set()


def test_two_default_applications_own_independent_workers(job_data):
    first_entered = Event()
    second_entered = Event()
    first_release = Event()
    second_release = Event()
    first_seen = []
    second_seen = []

    def first_processor(data, stop):
        first_seen.append(data)
        first_entered.set()
        first_release.wait()

    def second_processor(data, stop):
        second_seen.append(data)
        second_entered.set()
        second_release.wait()

    first = create_app(processor=first_processor)
    second = create_app(processor=second_processor)
    first_worker = first.state.worker
    second_worker = second.state.worker
    assert first_worker.queue is not second_worker.queue

    with TestClient(first) as first_client, TestClient(second) as second_client:
        try:
            first_data = {**job_data, "order_id": "FIRST"}
            second_data = {**job_data, "order_id": "SECOND"}
            first_response = first_client.post("/jobs", json=first_data)
            second_response = second_client.post("/jobs", json=second_data)
            assert first_response.status_code == second_response.status_code == 201
            first_id = first_response.json()["job_id"]
            second_id = second_response.json()["job_id"]
            assert first_entered.wait(2)
            assert second_entered.wait(2)
            assert first_client.get(f"/jobs/{second_id}").status_code == 404
            assert second_client.get(f"/jobs/{first_id}").status_code == 404
            for client, job_id in ((first_client, first_id), (second_client, second_id)):
                assert client.get(f"/jobs/{job_id}").json()["status"] == "processing"
            first_release.set()
            second_release.set()
            wait_for_tasks(first_worker.queue)
            wait_for_tasks(second_worker.queue)
            assert first_seen == [first_data]
            assert second_seen == [second_data]
            for client, job_id, data in (
                (first_client, first_id, first_data),
                (second_client, second_id, second_data),
            ):
                assert client.get(f"/jobs/{job_id}").json() == {
                    "job_id": job_id, "status": "completed", "data": data
                }
            assert first_worker.thread.is_alive()
            assert second_worker.thread.is_alive()
        finally:
            first_release.set()
            second_release.set()
    assert not first_worker.thread.is_alive()
    assert not second_worker.thread.is_alive()


@pytest.mark.parametrize("capacity", [0, -1])
def test_application_rejects_unbounded_queue_capacity(capacity):
    with pytest.raises(ValueError, match="positive integer"):
        create_app(queue_max_size=capacity)
