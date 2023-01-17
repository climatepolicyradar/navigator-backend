# Trigger bulk import from CSV

A simple script for triggering the processing pipeline via a bulk import.

## Execution

Run the command as follows after retrieving the values from the appropriate backend pulumi stack:

```shell
SUPERUSER_EMAIL="<pulumi.superuser_email>" SUPERUSER_PASSWORD="<pulumi.superuser_password>" API_HOST="https://<pulumi.api_domain>" python ./main.py <PATH_TO_CSV_FILE>
```

