"""
Pipeline Orchestrator (CLI Wrapper)
Usage: python run_pipeline.py [--steps step1,step2]
"""

import sys
import argparse
from src.utils.orchestrator import PipelineOrchestrator

def main():
    parser = argparse.ArgumentParser(description="YouTube Sentiment Pipeline CLI")
    parser.add_argument("--steps", help="Comma-separated list of steps to run", type=str)
    args = parser.parse_args()

    orchestrator = PipelineOrchestrator()
    all_available_steps = list(orchestrator.steps_config.keys())

    if args.steps:
        steps_to_run = [s.strip() for s in args.steps.split(",")]
        # Basic validation
        for s in steps_to_run:
            if s not in all_available_steps:
                print(f"Error: Step '{s}' is not available.")
                print(f"Available steps: {all_available_steps}")
                sys.exit(1)
    else:
        steps_to_run = all_available_steps

    print(f"Starting Pipeline Job with steps: {steps_to_run}")
    job_id = orchestrator.create_job(steps_to_run)
    print(f"Job ID: {job_id}")
    
    orchestrator.run_full_job(job_id, steps_to_run)
    
    job = orchestrator.get_job_status(job_id)
    print("\n" + "="*60)
    print(f"JOB STATUS: {job['status']}")
    print("="*60)
    
    if job["status"] == "failed":
        print("\nSee logs for details.")
        sys.exit(1)
    else:
        print("\nPipeline completed successfully.")

if __name__ == "__main__":
    main()
