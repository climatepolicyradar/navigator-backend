# Lambda Code for IAM User Creation

This Lambda function creates a PostgreSQL role that can authenticate via AWS IAM
for Aurora PostgreSQL.

## Prerequisites

The Aurora cluster must have:

- `iam_database_authentication_enabled = true`
- A matching IAM principal with `rds-db:connect` permission for the username and
  cluster

## Important Notes

- The DB username here must match the IAM RolePolicy resource suffix:
  `arn:aws:rds-db:REGION:ACCOUNT_ID:dbuser:CLUSTER_RESOURCE_ID/LOAD_DB_USER`
- Do NOT set a password here; IAM uses an auth token for login.
- Grant only the minimum privileges needed.

## SQL Script Steps

### 1) Ensure the AWS-provided IAM auth extension is available

Aurora PostgreSQL includes `aws_iam` on supported versions. If not present,
CREATE EXTENSION. On some engines, the extension is `aws_iam` and is
preinstalled so `CREATE EXTENSION IF NOT EXISTS` is harmless.

### 2) Create the role that will log in via IAM

The role name must exactly match the `load_db_user` configured in your Pulumi
and IAM policy. LOGIN is required. No password is set because IAM generates
temporary tokens.

### 3) Restrict connection to a specific database

The role is granted CONNECT privilege only on the specified database.

### 4) Grant minimal privileges required by our application

Currently grants USAGE on the specified schema. Additional privileges can be
granted as needed:

- If the loader only needs to insert into specific tables, grant precisely:

  ```sql
  GRANT INSERT ON TABLE public.your_table TO load_db_user;
  ```

- If it needs full DML on a schema, you could use:

  ```sql
  GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public
  TO load_db_user;
  ```

  And ensure future tables also carry appropriate ACLs:

  ```sql
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE,
  DELETE ON TABLES TO load_db_user;
  ```

## Template Variables

The SQL script uses template variables that are safely injected using psycopg2's
`sql.Identifier()`:

- `{LOAD_DB_USER}` - The IAM-authenticated database role name
- `{DB_NAME}` - The database name to grant CONNECT privilege on
- `{APP_SCHEMA}` - The schema name to grant USAGE privilege on
