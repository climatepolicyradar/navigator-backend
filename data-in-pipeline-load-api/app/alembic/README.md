# Alembic migration guide

This directory contains the Alembic configuration and migration scripts for the
`data-in-pipeline-load-api` service. Migrations are generated from the shared
`SQLModel` / SQLAlchemy models in the `data-in-models` package.

The key files are:

- `alembic.ini` – Alembic configuration.
- `migrations/env.py` – wiring to the database and SQLModel metadata.
- `migrations/` – actual revision scripts.
- `run_migrations.py` – helper used by the service to run `upgrade`.

All commands below assume you are in the service directory:

```bash
cd /Users/katy/Code/navigator-backend/data-in-pipeline-load-api
```

You should run Alembic via `uv` so that it uses the correct virtual environment:

```bash
uv run alembic -c app/alembic/alembic.ini --help
```

---

## Environment variables

`migrations/env.py` constructs the database URL from environment variables
rather than from `alembic.ini`. Before running any Alembic command you must
provide:

- `AURORA_WRITER_ENDPOINT` – database host.
- `DB_NAME` – database name.
- `DB_ADMIN_USER` – admin / superuser account for migrations.
- `DB_ADMIN_PASSWORD` – password for `DB_ADMIN_USER`.
- `DB_PORT` – database port, for example `5432`.

For a local Postgres instance you might use something along the lines of:

```bash
export AURORA_WRITER_ENDPOINT=localhost
export DB_NAME=data-in-pipeline-load-api
export DB_ADMIN_USER=data-in-pipeline-load-api
export DB_ADMIN_PASSWORD=data-in-pipeline-load-api
export DB_PORT=5432
```

---

## Generating a new revision

Alembic is configured to autogenerate migrations from the global
`SQLModel.metadata`. The high‑level flow is:

1. Change or add models in `data-in-models`.
2. Ensure the service environment is installed:

   ```bash
   uv sync --project data-in-pipeline-load-api
   ```

3. Set the environment variables described above so Alembic can connect to the
   desired database (typically a local Postgres instance that already has the
   previous migrations applied).
4. Run Alembic `revision --autogenerate`:

   ```bash
   uv run alembic \
     -c app/alembic/alembic.ini \
     revision --autogenerate \
     -m "short description of the change"
   ```

Notes specific to this repository:

- Revision identifiers are generated numerically by
  `generate_incremental_revision_id` in `migrations/env.py`. The next revision
  will be `current_head + 1`, formatted as four digits (for example `0001`,
  `0002`, and so on).
- If you omit `-m/--message`, `env.py` will refuse to create the revision and
  will log an informational message.
- If Alembic detects no schema changes relative to the database, it will skip
  creating a revision and log that no changes were found.

After generating a revision:

1. Open the new script under `app/alembic/migrations/`.
2. Review the `upgrade()` and `downgrade()` operations carefully; autogenerate
   is a starting point, not a guarantee.
3. Add any data migrations or manual tweaks that are needed.

---

## Applying and rolling back migrations manually

To upgrade the database to the latest revision:

```bash
uv run alembic \
  -c app/alembic/alembic.ini \
  upgrade head
```

To upgrade (or downgrade) to a specific revision:

```bash
uv run alembic \
  -c app/alembic/alembic.ini \
  upgrade 0003

uv run alembic \
  -c app/alembic/alembic.ini \
  downgrade 0002
```

To step backwards by a single revision:

```bash
uv run alembic \
  -c app/alembic/alembic.ini \
  downgrade -1
```

To see the current head(s) and history:

```bash
uv run alembic \
  -c app/alembic/alembic.ini \
  current

uv run alembic \
  -c app/alembic/alembic.ini \
  history
```

The application code uses `run_migrations.py` to apply migrations on start‑up by
calling `command.upgrade(..., "head")` with the pre‑configured `alembic.ini` and
`migrations/` location.

---

## Testing Alembic with `pytest-alembic`

We rely on `pytest-alembic` instead of hand‑rolled migration tests. The plugin
is installed in the `dev` dependency group in `pyproject.toml`, and the standard
test suite for this service lives in `tests/test_run_migrations.py`.

The test module simply re‑exports the upstream tests:

- `test_model_definitions_match_ddl`
- `test_single_head_revision`
- `test_upgrade`
- `test_up_down_consistency`
- `test_downgrade_leaves_no_trace` (experimental).

Alembic tests are marked with `@pytest.mark.alembic`. To run only the Alembic
tests from the service directory:

```bash
uv run pytest -vv tests/test_run_migrations.py -m alembic
```

Or, using the `just` target:

```bash
just run-alembic-tests
```

The `pytest` configuration in `pyproject.toml` defines an `alembic` marker to
make selection explicit:

```toml
[tool.pytest.ini_options]
markers = [
  "alembic: marks tests as alembic (deselect with '-m \"not alembic\"')",
]
```

`pytest-alembic` will discover the Alembic configuration via the
`alembic_runner` fixture and will exercise migrations end‑to‑end by:

- Migrating from base to head.
- Optionally downgrading and checking that no residual artefacts are left
  behind.
- Comparing the DDL produced by the models with the database schema.

When you add or alter migrations:

1. Create and review a new Alembic revision as described above.
2. Run the Alembic test suite:

   ```bash
   uv run pytest -vv tests/test_run_migrations.py -m alembic
   ```

3. Ensure that:
   - There is a single head revision
   - `upgrade` and `downgrade` both succeed
   - The model definitions match the database schema

Some tests are currently marked as `xfail` until the first working revision and
tables exist. Once the initial migrations are in place and passing, you should
remove the `xfail` markers so that regression failures are surfaced in CI.
