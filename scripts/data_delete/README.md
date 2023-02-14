# Trigger bulk delete from an endpoint

A simple script for triggering the deletion of data in s3 and the rds database.

## Execution

Run the command as follows after retrieving the values from the appropriate backend pulumi stack:

```shell
SUPERUSER_EMAIL="<pulumi.superuser_email>" SUPERUSER_PASSWORD="<pulumi.superuser_password>" API_HOST="https://<pulumi.api_domain>" python ./main.py "<comma separated document id string>"
```

