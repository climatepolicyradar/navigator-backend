-- This script creates a PostgreSQL role that can authenticate via AWS IAM
-- for Aurora PostgreSQL. It assumes the cluster has
-- iam_database_authentication_enabled = true
-- and that a matching IAM principal has rds-db:connect permission for this
-- username and cluster.

-- IMPORTANT:
-- - The DB username here must match the IAM RolePolicy resource suffix:
--   arn:aws:rds-db:REGION:ACCOUNT_ID:dbuser:CLUSTER_RESOURCE_ID/LOAD_DB_USER
-- - Do NOT set a password here; IAM uses an auth token for login.
-- - Grant only the minimum privileges needed.
BEGIN;

-- 1) Ensure the AWS-provided IAM auth extension is available.
-- Aurora PostgreSQL includes 'aws_iam' on supported versions. If not present,
-- CREATE EXTENSION.
-- On some engines, the extension is 'aws_iam' and is preinstalled so
-- CREATE EXTENSION IF NOT EXISTS is harmless.
CREATE EXTENSION IF NOT EXISTS aws_iam;

-- 2) Create the role that will log in via IAM.
-- The role name must exactly match the 'load_db_user' configured in your
-- Pulumi and IAM policy.
-- LOGIN is required. No password is set because IAM generates temporary tokens.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{LOAD_DB_USER}') THEN  -- noqa: PRS
    CREATE ROLE {LOAD_DB_USER} LOGIN;  -- noqa: PRS
  END IF;
END
$$;

COMMENT ON ROLE {LOAD_DB_USER} IS 'IAM-auth via aws_iam; no password.';  -- noqa: PRS

-- 3) Restrict connection to a specific database.
GRANT CONNECT ON DATABASE {DB_NAME} TO {LOAD_DB_USER};  -- noqa: PRS

-- 4) Grant minimal privileges required by our application.

-- If the loader only needs to insert into specific tables, grant precisely:
-- GRANT INSERT ON TABLE public.your_table TO load_db_user;

-- If it needs full DML on a schema, you could use:
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public
-- TO load_db_user;
-- And ensure future tables also carry appropriate ACLs:
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE,
-- DELETE ON TABLES TO load_db_user;
GRANT USAGE ON SCHEMA {APP_SCHEMA} TO {LOAD_DB_USER};  -- noqa: PRS

COMMIT;
