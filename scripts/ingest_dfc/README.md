# Ingest Document, Families and Collections (DFC)

This script is to go from the old schema (0010) to the new schema (0013) which includes collections and families.

## Background

- Marcus currently supplies a CSV document which is the source of truth for the CCLW data.
- This CSV is processed by the 
- 

## Script used to test

```
#!/bin/bash

clear
docker-compose down
docker volume rm navigator-backend_db-data-backend
docker-compose up -d backend_db
timeout 10s sh -c 'until psql -c "SELECT 1"; do sleep 0.7; done' localhost 5432
PYTHONPATH=$PWD alembic upgrade 0013
PYTHONPATH=$PWD python app/initial_data.py skip-wait
PYTHONPATH=$PWD python scripts/ingest_dfc/ingest_dfc.py ~/tmp/dfc_fixed_processed.csv
```
