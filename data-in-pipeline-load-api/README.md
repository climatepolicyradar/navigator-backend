# data-in-pipeline-load-api

FastAPI service backed by Aurora Postgres. Authenticates to Aurora using IAM
auth tokens; locally we simulate this with deterministic tokens (see
[Why we freeze time](#why-we-freeze-time)).

## Prerequisites

- Docker

## Running locally (from the root of the repo)

```bash
just dev data-in-pipeline-load-api
```

The app is available on `http://localhost:8080`. The `db` service auto-loads SQL
files from `.data_cache/` on first init if provided but the app will start fine
without them.

## Running tests (from the root of the repo)

```bash
just test data-in-pipeline-load-api
```

Tests run inside the `test` container against the `test-db` service. The
fixtures in `tests/conftest.py` reuse the production engine from
`app.session.get_engine()`, so tests exercise the same pool config, isolation
level, and `do_connect` event listener as production.

## Building images

The Dockerfile uses a multi-stage build with three stages:

- `base` — shared runtime: Python, system deps, app code, `uv sync`
- `dev` — `base` + `libfaketime` for time freezing; default for local dev and
  tests
- `prod` — `base` only; what ships to production

Compose builds target `dev` explicitly. The just recipe for production builds
passes `--target prod`. If a build path ever forgets to specify a target, the
default is the last stage in the file (`prod`) — this is intentional, so the
safe default is to build prod.

## Why we freeze time

Production authenticates to Aurora using short-lived IAM auth tokens generated
by `boto3.client("rds").generate_db_auth_token(...)`. The token is a SigV4
presigned URL whose signature is derived from AWS credentials, the target
hostname/port/user/region, and **the current timestamp**.

Locally, we want the same code path in `app/session.py` to run unchanged — but
local Postgres can't validate IAM tokens. The workaround:

1. Fix the AWS credentials, region, hostname, port, and username via env vars in
   `docker-compose.yml`.
2. Freeze the clock inside the container using `libfaketime`, set via
   `LD_PRELOAD` in the `dev` Dockerfile stage and the `FAKETIME` env var.
3. With every input pinned, `generate_db_auth_token()` returns the same string
   on every call.
4. Set that string as `POSTGRES_PASSWORD` on the local `db` container. Postgres
   treats it as an opaque password; the app generates an identical string at
   runtime; auth succeeds.

The `dev` and `test` services follow the same pattern. Their tokens differ only
because the hostname differs (`db` vs `test-db`) and that hostname is part of
the signed payload.

### Dependencies between values

The committed `POSTGRES_PASSWORD` strings depend on:

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
- `AWS_REGION`
- `FAKETIME`
- `DB_URL` (used as `DBHostname`)
- `DB_PORT`
- `DB_USERNAME`

If any of these change, both `POSTGRES_PASSWORD` values need regenerating from
inside a running container with the new values.

## Adding local data

To add data to the local DB, connect to the production Aurora cluster via the
bastion and run `pg_dump`, then place the generated .sql file in a .data_cache
directory within this projectTo regenerate, connect to the production Aurora
cluster via the bastion (see the platform team's runbook) and run `pg_dump` with
a Postgres client whose major version matches Aurora's.
