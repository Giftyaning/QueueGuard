from queue import Full
from uuid import uuid4

from fastapi import APIRouter, FastAPI, HTTPException

from app.body_limit import MAX_JOB_BODY_BYTES, JobBodyLimitRoute
from app.schemas import CreateJobRequest
from app.job_queue import job_queue

app = FastAPI()
jobs_router = APIRouter(route_class=JobBodyLimitRoute)


@app.get("/health")
def health_check():
    return {"status": "ok"}


@jobs_router.post(
    "/jobs",
    status_code=201,
    responses={
        413: {"description": f"Job payload exceeds {MAX_JOB_BODY_BYTES} bytes"},
        503: {"description": "Job queue is full"},
    },
)
def create_job(request: CreateJobRequest):
    job_id = str(uuid4())

    job = {
        "job_id": job_id,
        "status": "queued",
        "data": request.model_dump()
    }

    try:
        job_queue.put_nowait(job)
    except Full:
        raise HTTPException(status_code=503, detail="Job queue is full") from None

    return {
        "job_id": job_id,
        "status": "queued"
    }


app.include_router(jobs_router)
