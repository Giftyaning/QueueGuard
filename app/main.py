from fastapi import FastAPI
from uuid import uuid4
from app.schemas import CreateJobRequest
from app.job_queue import job_queue

app = FastAPI()


@app.get("/health")
def health_check():
    return {"status": "ok"}



@app.post("/jobs", status_code=201)
def create_job(request: CreateJobRequest):
    job_id = str(uuid4())

    job = {
        "job_id": job_id,
        "status": "queued",
        "data": request.model_dump()
    }

    job_queue.put(job)

    return {
        "job_id": job_id,
        "status": "queued"
    }
