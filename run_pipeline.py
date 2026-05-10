"""
Pipeline Orchestrator
Equivalent to running: kitchen.sh -file=run_pipeline.kjb
"""

import subprocess
import sys
import time
from datetime import datetime

def run_step(name, command, cwd="."):
    print(f"\n{'='*60}")
    print(f"STEP: {name}")
    print(f"Started: {datetime.now()}")
    print('='*60)
    
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
    print("YOUTUBE SENTIMENT PIPELINE")
    print("Equivalent to: kitchen.sh -file=run_pipeline.kjb")
    print("="*60)
    print(f"Started: {datetime.now()}")
    
    steps = [
        ("Producer", "python src/producer/youtube_producer.py", 120),
        ("Bronze", "python src/spark/bronze_batch.py", None),
        ("Silver", "python src/spark/bronze_to_silver.py", None),
        ("Gold", "python src/spark/silver_to_gold_hf.py", None),
        ("Pentaho Aggregation", "python pentaho/run_pentaho_batch.py", None),
    ]
    
    for name, command, duration in steps:
        if name == "Producer" and duration:
            print(f"\nRunning producer for {duration} seconds...")
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