"""Generate test metrics for dashboard exploration."""

import os
import random
import time

# Ensure we're in development environment
os.environ["ENV"] = "development"

from app.bootstrap_telemetry import pipeline_metrics
from app.pipeline_metrics import ErrorType, Operation, PipelineType, Status

# Generate some test data
NUM_ITERATIONS = 1000
for i in range(NUM_ITERATIONS):
    # Simulate successful/failed processing
    status = random.choice(
        [Status.SUCCESS, Status.SUCCESS, Status.SUCCESS, Status.FAILURE]
    )
    pipeline_metrics.record_processed(PipelineType.DOCUMENT, status)

    # Simulate operation durations
    for op in Operation:
        duration = random.uniform(0.1, 5.0)
        pipeline_metrics.record_duration(op, duration)

    # Simulate some errors
    if random.random() < 0.1:
        op = random.choice(list(Operation))
        err = random.choice(list(ErrorType))
        pipeline_metrics.record_error(op, err)

    if i % 10 == 0:
        print(f"{i}/{NUM_ITERATIONS} -- Sleeping for 60s...")
        time.sleep(60)

print("Test metrics generated. Waiting for export...")
time.sleep(10)  # Wait for periodic export
print("Done")
