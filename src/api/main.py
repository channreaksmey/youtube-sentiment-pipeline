import sys
import os
import logging
from typing import List, Optional, Dict
from datetime import datetime
from pydantic import BaseModel
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query

# Ensure project root is in sys.path
sys.path.append(os.getcwd())

from src.utils.orchestrator import PipelineOrchestrator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("pipeline-api")

app = FastAPI(
    title="YouTube Sentiment Pipeline API",
    description="Refactored API to orchestrate the YouTube comment sentiment ETL pipeline with persistent job tracking."
)

orchestrator = PipelineOrchestrator()

class JobCreate(BaseModel):
    steps: Optional[List[str]] = None  # If None, run all steps

@app.get("/")
async def root():
    return {
        "message": "YouTube Sentiment Pipeline API",
        "status": "online",
        "available_steps": list(orchestrator.steps_config.keys()),
        "timestamp": datetime.now()
    }

@app.post("/jobs", status_code=202)
async def trigger_job(job_data: JobCreate, background_tasks: BackgroundTasks):
    """
    Triggers a pipeline job with the specified steps (or all steps).
    """
    all_steps = list(orchestrator.steps_config.keys())
    steps_to_run = job_data.steps if job_data.steps else all_steps
    
    # Validate steps
    for step in steps_to_run:
        if step not in orchestrator.steps_config:
            raise HTTPException(status_code=400, detail=f"Invalid step: {step}")
    
    job_id = orchestrator.create_job(steps_to_run)
    background_tasks.add_task(orchestrator.run_full_job, job_id, steps_to_run)
    
    return {
        "job_id": job_id,
        "status": "accepted",
        "steps": steps_to_run,
        "message": "Job triggered successfully"
    }

@app.get("/jobs")
async def list_jobs(limit: int = Query(10, ge=1, le=50)):
    """
    Returns a list of recent jobs.
    """
    return {"jobs": orchestrator.list_jobs(limit)}

@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """
    Returns the full status and logs for a specific job.
    """
    job = orchestrator.get_job_status(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.get("/steps")
async def list_steps():
    """
    Returns a list of all available pipeline steps.
    """
    return {"steps": list(orchestrator.steps_config.keys())}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
