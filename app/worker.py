"""In-process job lifecycle and cooperative background processing."""

from copy import deepcopy
import logging
from queue import Empty, Queue
from threading import Event, Lock, Thread
from typing import Callable
from uuid import uuid4


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

    def __init__(self, queue: Queue, processor: Processor = process_job):
        self.queue = queue
        self.processor = processor
        self._jobs: dict[str, dict] = {}
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
            return deepcopy(self._jobs.get(job_id))

    def _set_status(self, job_id: str, status: str) -> None:
        with self._lock:
            self._jobs[job_id]["status"] = status

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
            try:
                job = self.queue.get(timeout=0.05)
            except Empty:
                continue
            try:
                self._set_status(job["job_id"], "processing")
                self.processor(deepcopy(job["data"]), self.stop_requested)
                self._set_status(job["job_id"], "completed")
            except Exception as error:
                self._set_status(job["job_id"], "failed")
                # Exception messages/tracebacks may contain request values.
                logger.error("Job %s failed (%s)", job["job_id"], type(error).__name__)
            finally:
                self.queue.task_done()
