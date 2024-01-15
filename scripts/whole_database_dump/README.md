# Whole database dump script

This script has been created as part of the ['Data download as CSV'](https://www.notion.so/climatepolicyradar/Brief-Data-download-as-CSV-ef071242e1424f5a953fd8c2de2e8526) work.

## Prerequisites

You need to be logged into AWS via the CLI. If you are not, login using `aws sso login`. You will also need to have a connection open to the RDS instance you wish to take a dump of. If you want to take a dump of either the staging or prod RDS instances, run the `nav-stack-connect.sh` script and follow the instructions (sourcing environment variables, running the socat command etc) to open the connection before running this script.

The following Python packages are required to run the script:
* boto3
* pandas
* sqlalchemy

You'll also need to set the below environment variables:
* `DATABASE_URL`, in the format `postgresql://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/${PGDATABASE}` where the values of the variables `PGDATABASE`, `PGUSER`, `PGPASSWORD` are taken from Pulumi.
* `AWS_PROFILE` (the `AWS_ENVIRONMENT` variable in the script uses the profile name to determine whether we are on staging or prod - the script assumes staging unless the substring 'prod' appears in the profile name).
