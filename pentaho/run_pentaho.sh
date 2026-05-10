#!/bin/bash
# Run Pentaho Job (.kjb) inside Docker

set -e

echo "=========================================="
echo "Running Pentaho Job: run_pipeline.kjb"
echo "=========================================="

# Check if container is running
if ! docker ps | grep -q "pentaho-pdi"; then
    echo "Starting Pentaho container..."
    docker-compose up -d pentaho
    sleep 10
fi

# Run the job
docker exec -it pentaho-pdi /opt/pentaho-pdi/kitchen.sh \
    -file=/pentaho-jobs/run_pipeline.kjb \
    -level=Basic

echo "=========================================="
echo "Pentaho Job Complete"
echo "=========================================="