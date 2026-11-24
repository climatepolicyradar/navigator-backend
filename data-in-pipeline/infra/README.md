# Data in Pipeline Infra

## Connecting to Aurora

The bastion lives in the internal-tools repo, in
[`tools/bastion`](https://github.com/climatepolicyradar/internal-tools/tree/main/tools/bastion).
This stack allows it to reach the Aurora cluster on 5432.

1. Follow the "Connecting to Aurora" steps in the bastion README to open the
   port forward. It listens on local port 5434.

2. Connect to Aurora by typing the following into a new terminal window

   ```bash
   export PGPORT=5434
   export PGUSER=data_in_pipeline
   export PGDATABASE=data_in_pipeline_load
   export PGHOST=localhost
   export PGPASSWORD=GET_THIS_FROM_THE_PULUMI_CONFIG
   psql
   ```

   You should see the connection accepted in the terminal running the port
   forward.
