"""Generate test metrics for dashboard exploration."""

# nosec: B311

import os
import random
import time

from app.bootstrap_telemetry import pipeline_metrics
from app.pipeline_metrics import ErrorType, Operation, PipelineType, Status

# Ensure we're in development environment
os.environ["ENV"] = "development"

# Generate some test data
NUM_ITERATIONS = 1000
for i in range(NUM_ITERATIONS):
    # Simulate successful/failed processing
    status = random.choice(  # nosec: B311
        [Status.SUCCESS, Status.SUCCESS, Status.SUCCESS, Status.FAILURE]
    )

    # Simulate operation durations
    for op in Operation:
        duration = random.uniform(0.1, 5.0)  # nosec: B311
        pipeline_metrics.record_processed(
            PipelineType.DOCUMENT, op, status, random.randint(1, 100)  # nosec: B311
        )
        pipeline_metrics.record_duration(op, duration)

    # Simulate some errors
    if random.random() < 0.1:  # nosec: B311
        op = random.choice(list(Operation))  # nosec: B311
        err = random.choice(list(ErrorType))  # nosec: B311
        pipeline_metrics.record_error(op, err)

    if i % 10 == 0:
        print(f"{i}/{NUM_ITERATIONS} -- Sleeping for 60s...")
        time.sleep(60)

print("Test metrics generated. Waiting for export...")
time.sleep(10)  # Wait for periodic export
print("Done")
