import subprocess
import sys
import time
import os
import json
import uuid
import logging
from datetime import datetime
import psycopg2
from typing import List, Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Orchestrator")

# Import config (assuming we run from root)
sys.path.append(os.getcwd())
from src.utils.config import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, 
    POSTGRES_USER, POSTGRES_PASSWORD
)

DB_CONFIG = {
    "host": POSTGRES_HOST,
    "port": POSTGRES_PORT,
    "database": POSTGRES_DB,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD
}

class PipelineOrchestrator:
    def __init__(self):
        self.steps_config = {
            "Database Setup": {"command": f"{sys.executable} setup_database.py", "type": "python"},
            "Producer Simulator": {"command": f"{sys.executable} src/producer/youtube_simulator.py", "type": "producer", "duration": 10},
            "Bronze": {"command": "src/spark/bronze_batch.py", "type": "spark"},
            "Silver": {"command": "src/spark/bronze_to_silver.py", "type": "spark"},
            "Gold (Emotion)": {"command": "src/spark/gold_emotion_pipeline.py", "type": "spark"},
            "Pentaho Aggregation": {"command": f"{sys.executable} pentaho/run_pentaho_batch.py", "type": "python"},
        }

    def _get_db_conn(self):
        return psycopg2.connect(**DB_CONFIG)

    def create_job(self, steps: List[str]) -> str:
        job_id = str(uuid.uuid4())[:8]
        conn = self._get_db_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO pipeline_jobs (job_id, steps, status, start_time) VALUES (%s, %s, %s, %s)",
            (job_id, json.dumps(steps), "running", datetime.now())
        )
        conn.commit()
        cur.close()
        conn.close()
        return job_id

    def update_job_status(self, job_id: str, status: str, logs: str):
        conn = self._get_db_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE pipeline_jobs SET status = %s, logs = %s, end_time = %s WHERE job_id = %s",
            (status, logs, datetime.now() if status in ["completed", "failed"] else None, job_id)
        )
        conn.commit()
        cur.close()
        conn.close()

    def run_step(self, name: str, job_id: str) -> str:
        config = self.steps_config.get(name)
        if not config:
            return f"Error: Unknown step {name}\n"

        output = f"\n{'='*60}\nSTEP: {name}\nStarted: {datetime.now()}\n{'='*60}\n"
        
        command = config["command"]
        
        if config["type"] == "spark":
            # Add resource limits for laptop-friendly execution
            script_path = command
            docker_path = f"/opt/spark/work-dir/{script_path}"
            
            packages = ""
            if "bronze_batch" in script_path:
                packages = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
            elif "silver" in script_path or "gold" in script_path:
                packages = "--packages org.postgresql:postgresql:42.7.3"
                
            ivy_conf = "--conf spark.driver.extraJavaOptions=-Divy.home=/opt/spark/work-dir/.ivy2 --conf spark.executor.extraJavaOptions=-Divy.home=/opt/spark/work-dir/.ivy2"
            python_path = "PYTHONPATH=/opt/spark/work-dir"
            
            # Resource limits: 1g memory for driver and executor
            resource_limits = "--driver-memory 1g --executor-memory 1g"
            
            command = f"docker exec -t -e {python_path} spark /opt/spark/bin/spark-submit {packages} {ivy_conf} {resource_limits} --conf spark.executorEnv.{python_path} --conf spark.driverEnv.{python_path} {docker_path}"
            output += f"Executing in Docker with resource limits: {command}\n"

        if config["type"] == "producer":
            duration = config.get("duration", 10)
            output += f"Running {name} for {duration} seconds...\n"
            try:
                # Use a separate process group if possible, but on Windows taskkill /T is easier
                proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                time.sleep(duration)
                
                # Robust termination for Windows
                if os.name == 'nt':
                    subprocess.run(f"taskkill /F /T /PID {proc.pid}", shell=True, capture_output=True)
                else:
                    proc.terminate()
                
                try:
                    stdout, stderr = proc.communicate(timeout=5)
                    output += stdout + stderr
                except subprocess.TimeoutExpired:
                    proc.kill()
                    stdout, stderr = proc.communicate()
                    output += stdout + stderr
                
                output += f"Producer stopped\n"
                return output
            except Exception as e:
                output += f"Producer failed: {str(e)}\n"
                raise Exception(output)

        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                check=True
            )
            output += result.stdout
            output += f"{name} completed successfully\n"
            return output
        except subprocess.CalledProcessError as e:
            output += f"STDOUT: {e.stdout}\nSTDERR: {e.stderr}\n"
            output += f"{name} failed with exit code {e.returncode}\n"
            raise Exception(output)

    def run_full_job(self, job_id: str, steps: List[str]):
        full_logs = ""
        try:
            for step_name in steps:
                step_log = self.run_step(step_name, job_id)
                full_logs += step_log
                self.update_job_status(job_id, "running", full_logs)
            
            self.update_job_status(job_id, "completed", full_logs)
            logger.info(f"Job {job_id} completed successfully")
        except Exception as e:
            full_logs += f"\nPIPELINE ERROR:\n{str(e)}"
            self.update_job_status(job_id, "failed", full_logs)
            logger.error(f"Job {job_id} failed")

    def get_job_status(self, job_id: str) -> Optional[Dict]:
        conn = self._get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT job_id, steps, status, start_time, end_time, logs FROM pipeline_jobs WHERE job_id = %s", (job_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row:
            return {
                "job_id": row[0],
                "steps": json.loads(row[1]),
                "status": row[2],
                "start_time": row[3],
                "end_time": row[4],
                "logs": row[5]
            }
        return None

    def list_jobs(self, limit: int = 10) -> List[Dict]:
        conn = self._get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT job_id, status, start_time, end_time FROM pipeline_jobs ORDER BY start_time DESC LIMIT %s", (limit,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        return [
            {"job_id": r[0], "status": r[1], "start_time": r[2], "end_time": r[3]}
            for r in rows
        ]
