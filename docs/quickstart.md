# Development

You have a choice between local (host-based) or docker-based development.

## Local development

For development on your local machine (not using containers)

```shell
make dev_install
```

This will install pip, poetry, trunk and it's child tools, and the pre-commit
hooks, and also set up a poetry environment for the backend.

It will also create an [environment](./environment.md) file at `.env`.

## Installing the right version of db-client for local development

The correct version is installed in the docker containers, however it can be
tricky to get that version in your local environment, so use:

```bash
pip install git+https://github.com/climatepolicyradar/navigator-db-client@v3.4.0
```

## Docker-based development

The only dependencies for docker-based development should be docker and docker-compose.

Firstly, copy the sample [environment](./environment.md) file:

```shell
cp .env.example .env
```

Then run the following command:

```shell
make start
make show_logs
```

This will build and bring up the containers, run database migrations and
populate the database with initial data.

## Running services

The backend will be at `http://localhost:8888`

Auto-generated docs will be at `http://localhost:8888/api/docs`

## Common issues

### Tests pass locally, but not on CI

CI uses docker, so ensure all containers are built with the latest dependencies,
and all tests pass:

```shell
make build
make test
```

Missing code might be

- ignored by `.gitignore` and cannot be seen or found by CI
- ignored by `.dockerignore` and cannot be seen or found by the containers

## Further information

- [environment](./environment.md)
- [testing](./testing.md)
- [VSCode](./vscode.md)
