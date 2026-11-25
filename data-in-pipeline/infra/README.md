# Data in Pipeline Infra

## Connecting to Aurora

The bastion lives in the internal-tools repo, in
[`tools/bastion`](https://github.com/climatepolicyradar/internal-tools/tree/main/tools/bastion).
This stack allows it to reach the Aurora cluster on 5432.

1. Follow the "Connecting to Aurora" steps in the bastion README to open the
   port forward. It listens on local port 5434.

2. When the two terminals open, copy the following socat command into the
   terminal window with the ASCII text (ignoring the one it recommends you to
   use):

   ```bash
   # Where ${ENVIRONMENT} is the AWS environment
   # Look in AWS Aurora console for the writer cluster endpoint to replace
   # XXXXXXXXXXXX
   socat TCP-LISTEN:5432,reuseaddr,fork TCP4:data-in-pipeline-${ENVIRONMENT}-aurora-cluster.cluster-XXXXXXXXXXXX.eu-west-1.rds.amazonaws.com:5432
   ```

3. Connect to Aurora by typing the following into a new terminal window

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
