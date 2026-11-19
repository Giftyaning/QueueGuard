"""In-process job lifecycle and cooperative background processing."""

from collections import OrderedDict
from copy import deepcopy
import logging
from queue import Empty, Queue
from threading import Event, Lock, Thread
from time import monotonic
from typing import Callable
from uuid import uuid4

from app.job_queue import DEFAULT_STATUS_RETENTION_SECONDS, validate_status_retention_seconds


logger = logging.getLogger(__name__)
Processor = Callable[[dict, Event], None]


def process_job(data: dict, stop_requested: Event) -> None:
    """Exercise local serialization only. Completion implies no business outcome."""
    if stop_requested.is_set():
        raise InterruptedError("Processing cancelled during shutdown")
    # A deterministic, side-effect-free placeholder. No payment is performed.
    tuple(sorted(data.items()))


class JobWorker:
    """Own a queue exclusively, together with its status store and consumer."""

    def __init__(
        self,
        queue: Queue,
        processor: Processor = process_job,
        *,
        status_retention_seconds: float = DEFAULT_STATUS_RETENTION_SECONDS,
        clock: Callable[[], float] = monotonic,
    ):
        self.status_retention_seconds = validate_status_retention_seconds(status_retention_seconds)
        self.queue = queue
        self.processor = processor
        self._jobs: dict[str, dict] = {}
        # Fixed retention + monotonic terminal transitions keep deadlines ordered.
        self._expires_at: OrderedDict[str, float] = OrderedDict()
        self._clock = clock
        self._cleanup_interval = min(60.0, self.status_retention_seconds)
        self._next_cleanup = self._clock() + self._cleanup_interval
        self._lock = Lock()
        self.stop_requested = Event()
        self.thread: Thread | None = None

    def submit(self, data: dict) -> dict:
        job = {"job_id": str(uuid4()), "status": "queued", "data": deepcopy(data)}
        with self._lock:
            if self.stop_requested.is_set():
                raise RuntimeError("Worker is stopping")
            # Register first, rolling back if the bounded queue rejects the job.
            self._jobs[job["job_id"]] = job
            try:
                self.queue.put_nowait(deepcopy(job))
            except Exception:
                del self._jobs[job["job_id"]]
                raise
        return {"job_id": job["job_id"], "status": "queued"}

    def get_job(self, job_id: str) -> dict | None:
        with self._lock:
            deadline = self._expires_at.get(job_id)
            if deadline is not None and self._clock() >= deadline:
                del self._expires_at[job_id]
                del self._jobs[job_id]
                return None
            return deepcopy(self._jobs.get(job_id))

    def cleanup_expired(self) -> None:
        """Run scheduled cleanup; inspect only the ordered terminal expiry index."""
        with self._lock:
            now = self._clock()
            if now < self._next_cleanup:
                return
            while self._expires_at:
                job_id, deadline = next(iter(self._expires_at.items()))
                if deadline > now:
                    break
                del self._expires_at[job_id]
                del self._jobs[job_id]
            self._next_cleanup = now + self._cleanup_interval

    def _set_status(self, job_id: str, status: str) -> None:
        with self._lock:
            self._jobs[job_id]["status"] = status
            if status in ("completed", "failed"):
                self._expires_at[job_id] = self._clock() + self.status_retention_seconds

    def start(self) -> None:
        if self.thread is not None:
            raise RuntimeError("Worker has already been started")
        self.thread = Thread(target=self._run, name="queueguard-worker", daemon=True)
        self.thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        # Serialize against submit so nothing is accepted after shutdown begins.
        with self._lock:
            self.stop_requested.set()
        if self.thread is not None:
            self.thread.join(timeout=timeout)
            if self.thread.is_alive():
                raise RuntimeError("Job processor did not stop within the shutdown timeout")

    def _run(self) -> None:
        while not self.stop_requested.is_set():
            self.cleanup_expired()
            try:
                # Keep shutdown responsive without a second cleanup thread.
                job = self.queue.get(timeout=0.25)
            except Empty:
                continue
            try:
                self._set_status(job["job_id"], "processing")
                self.processor(deepcopy(job["data"]), self.stop_requested)
            except Exception as error:
                self._set_status(job["job_id"], "failed")
                # Exception messages/tracebacks may contain request values.
                logger.error("Job %s failed (%s)", job["job_id"], type(error).__name__)
            else:
                self._set_status(job["job_id"], "completed")
            finally:
                self.queue.task_done()
