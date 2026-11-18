from contextlib import asynccontextmanager
from queue import Full, Queue

from fastapi import APIRouter, FastAPI, HTTPException

from app.body_limit import MAX_JOB_BODY_BYTES, JobBodyLimitRoute
from app.job_queue import QUEUE_MAX_SIZE
from app.schemas import CreateJobRequest, JobAcceptedResponse, JobStatusResponse
from app.worker import JobWorker, Processor, process_job


def create_app(
    *,
    queue_max_size: int = QUEUE_MAX_SIZE,
    processor: Processor = process_job,
    start_worker: bool = True,
) -> FastAPI:
    """Own one queue, status store and worker; inject capacity and processing only."""
    if queue_max_size < 1:
        raise ValueError("queue_max_size must be a positive integer")
    worker = JobWorker(Queue(maxsize=queue_max_size), processor)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if start_worker:
            worker.start()
        try:
            yield
        finally:
            worker.stop()

    app = FastAPI(
        lifespan=lifespan,
        title="QueueGuard",
        description=(
            "In-process prototype job queue. Completion means only that the local "
            "placeholder finished; no payment or business operation is performed."
        ),
    )
    app.state.worker = worker
    jobs_router = APIRouter(route_class=JobBodyLimitRoute)

    @app.get("/health")
    def health_check():
        return {"status": "ok"}

    @jobs_router.post(
        "/jobs",
        status_code=201,
        response_model=JobAcceptedResponse,
        responses={
            413: {"description": f"Job payload exceeds {MAX_JOB_BODY_BYTES} bytes"},
            503: {"description": "Job queue is full or shutting down"},
        },
    )
    def create_job(request: CreateJobRequest):
        """Accept a job into the bounded waiting queue for background processing."""
        try:
            return worker.submit(request.model_dump())
        except Full:
            raise HTTPException(status_code=503, detail="Job queue is full") from None
        except RuntimeError:
            raise HTTPException(status_code=503, detail="Job worker is stopping") from None

    @app.get(
        "/jobs/{job_id}",
        response_model=JobStatusResponse,
        responses={404: {"description": "Unknown job ID"}},
    )
    def get_job(job_id: str):
        """Return the current lifecycle state and the accepted, normalized data."""
        job = worker.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return job

    app.include_router(jobs_router)
    return app


app = create_app()
