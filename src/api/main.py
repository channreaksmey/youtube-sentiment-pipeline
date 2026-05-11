from fastapi import FastAPI, BackgroundTasks, HTTPException
import subprocess
import os
import sys
from datetime import datetime
import logging
from typing import Dict, Optional, List
import uuid

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("pipeline-api")

app = FastAPI(
    title="YouTube Sentiment Pipeline API",
    description="API to orchestrate the YouTube comment sentiment ETL pipeline"
)

# Configuration
ORCHESTRATOR_PATH = "run_pipeline.py"

# In-memory state tracking
class PipelineState:
    def __init__(self):
        self.is_running = False
        self.current_job_id: Optional[str] = None
        self.history: List[Dict] = []
        self.last_logs: str = ""

state = PipelineState()

def run_pipeline_task(job_id: str):
    state.is_running = True
    state.current_job_id = job_id
    
    start_time = datetime.now()
    job_info = {
        "job_id": job_id,
        "start_time": start_time,
        "status": "running",
        "end_time": None,
        "error": None
    }
    state.history.append(job_info)
    
    logger.info(f"Job {job_id}: Pipeline execution started")
    
    try:
        # Run the orchestrator script
        # We capture output to store in state.last_logs
        result = subprocess.run(
            [sys.executable, ORCHESTRATOR_PATH],
            capture_output=True,
            text=True,
            check=True
        )
        state.last_logs = result.stdout
        job_info["status"] = "completed"
        logger.info(f"Job {job_id}: Pipeline execution completed successfully")
        
    except subprocess.CalledProcessError as e:
        state.last_logs = f"STDOUT:\n{e.stdout}\n\nSTDERR:\n{e.stderr}"
        job_info["status"] = "failed"
        job_info["error"] = str(e.stderr)
        logger.error(f"Job {job_id}: Pipeline execution failed: {e.stderr}")
        
    except Exception as e:
        state.last_logs = str(e)
        job_info["status"] = "error"
        job_info["error"] = str(e)
        logger.error(f"Job {job_id}: An unexpected error occurred: {e}")
        
    finally:
        job_info["end_time"] = datetime.now()
        state.is_running = False
        state.current_job_id = None

@app.get("/")
async def root():
    return {
        "message": "YouTube Sentiment Pipeline API",
        "status": "online",
        "pipeline_running": state.is_running,
        "timestamp": datetime.now()
    }

@app.post("/pipeline/run", status_code=202)
async def trigger_pipeline(background_tasks: BackgroundTasks):
    """
    Triggers the full pipeline in the background.
    """
    if state.is_running:
        raise HTTPException(
            status_code=409, 
            detail=f"Pipeline is already running (Job ID: {state.current_job_id})"
        )
    
    job_id = str(uuid.uuid4())[:8]
    background_tasks.add_task(run_pipeline_task, job_id)
    
    return {
        "job_id": job_id,
        "status": "accepted",
        "message": "Pipeline triggered successfully",
        "estimated_duration": "Approx 3-5 minutes"
    }

@app.get("/pipeline/status")
async def get_status():
    """
    Returns the current status of the pipeline and the last few jobs.
    """
    return {
        "is_running": state.is_running,
        "current_job_id": state.current_job_id,
        "recent_history": state.history[-5:] if state.history else []
    }

@app.get("/pipeline/logs")
async def get_logs():
    """
    Returns the logs from the last execution.
    """
    if not state.last_logs:
        return {"message": "No logs available yet. Run the pipeline first."}
    return {"logs": state.last_logs}

@app.get("/pipeline/history")
async def get_history():
    """
    Returns the full execution history.
    """
    return {"history": state.history}

if __name__ == "__main__":
    import uvicorn
    # Check if run_pipeline.py exists in the current directory
    if not os.path.exists(ORCHESTRATOR_PATH):
        logger.error(f"Could not find orchestrator at {ORCHESTRATOR_PATH}. Make sure you run this from the project root.")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)
