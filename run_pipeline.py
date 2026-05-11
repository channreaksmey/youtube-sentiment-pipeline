"""
Pipeline Orchestrator
Equivalent to running: kitchen.sh -file=run_pipeline.kjb
"""

import subprocess
import sys
import time
import os
from datetime import datetime

def run_step(name, command, cwd="."):
    print(f"\n{'='*60}")
    print(f"STEP: {name}")
    print(f"Started: {datetime.now()}")
    print('='*60)
    
    # Check if we should run via docker exec for spark jobs
    if "src/spark/" in command and "python" in command:
        script_path = command.split(" ")[-1]
        docker_path = f"/opt/spark/work-dir/{script_path}"
        
        # Add packages for spark-submit
        packages = ""
        ivy_conf = "--conf spark.driver.extraJavaOptions=-Divy.home=/opt/spark/work-dir/.ivy2 --conf spark.executor.extraJavaOptions=-Divy.home=/opt/spark/work-dir/.ivy2"
        
        if "bronze_batch" in script_path:
            packages = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        elif "silver" in script_path or "gold" in script_path:
            packages = "--packages org.postgresql:postgresql:42.7.3"
            
        # Use full path to spark-submit just in case
        python_path = "PYTHONPATH=/opt/spark/work-dir"
        command = f"docker exec -t -e {python_path} spark /opt/spark/bin/spark-submit {packages} {ivy_conf} --conf spark.executorEnv.{python_path} --conf spark.driverEnv.{python_path} {docker_path}"
        print(f"Executing in Docker: {command}")

    try:
        result = subprocess.run(
            command,
            shell=True,
            cwd=cwd,
            check=True,
            capture_output=False,
            text=True
        )
        print(f"{name} completed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"{name} failed: {e}")
        return False

def main():
    print("="*60)
    print("YOUTUBE SENTIMENT PIPELINE (DOCKER SPARK)")
    print("="*60)
    print(f"Started: {datetime.now()}")
    
    steps = [
        ("Database Setup", "python setup_database.py", None),
        # ("Producer", "python src/producer/youtube_producer.py", 120),
        ("Producer Simulator", "python src/producer/youtube_simulator.py", 10),
        ("Bronze", "python src/spark/bronze_batch.py", None),
        ("Silver", "python src/spark/bronze_to_silver.py", None),
        ("Gold (Emotion)", "python src/spark/gold_emotion_pipeline.py", None),
        ("Pentaho Aggregation", "python pentaho/run_pentaho_batch.py", None),
    ]
    
    for name, command, duration in steps:
        if "Producer" in name and duration:
            print(f"\nRunning {name} for {duration} seconds...")
            proc = subprocess.Popen(command, shell=True)
            time.sleep(duration)
            proc.terminate()
            proc.wait()
            print(f"Producer stopped")
        else:
            success = run_step(name, command)
            if not success:
                print(f"\nPipeline failed at: {name}")
                sys.exit(1)
    
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print(f"Finished: {datetime.now()}")
    print("\nRun dashboard: streamlit run src/dashboard/app.py")

if __name__ == "__main__":
    main()
