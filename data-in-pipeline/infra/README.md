# Data in Pipeline Infra

## Connecting to the LHCI RDS

1. Run the [`nav-stack-connect`](../backend/nav-stack-connect.sh) script to
   connect to the bastion from the backend infra folder

2. When the two terminals open, copy the following socat command into the
   terminal window with the ASCII text (ignoring the one it recommends you to
   use):

   ```bash
   # Where ${ENVIRONMENT} is the AWS environment
   # Look in AWS Aurora console for the writer cluster endpoint to replace
   # XXXXXXXXXXXX
   socat TCP-LISTEN:5432,reuseaddr,fork TCP4:data-in-pipeline-${ENVIRONMENT}-aurora-cluster.cluster-XXXXXXXXXXXX.eu-west-1.rds.amazonaws.com:5432
   ```

3. Connect to the RDS instance by typing the following into a new terminal
   window

   ```bash
   export PGPORT=5434
   export PGUSER=data_in_pipeline
   export PGDATABASE=data_in_pipeline_load
   export PGHOST=localhost
   export PGPASSWORD=GET_THIS_FROM_THE_PULUMI_CONFIG
   psql
   ```

   You should be able to see that the connection to the session has been
   accepted in the other terminal that the nav-stack-connect script opened.
