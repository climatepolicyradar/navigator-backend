# `navigator-backend`

## Installation

```bash
# cd MICROSERVICE_FOLDER
cd backend-api
uv sync
```

### Prerequisites

- Python 3.12+
- [Docker](https://www.docker.com/get-started/)
- [just](https://github.com/casey/just?tab=readme-ov-file#installation)
- [uv](<[https://](https://docs.astral.sh/uv/)>)

## Development

e.g.

```bash
# just dev {{service}}
just dev concepts-api
```

## Deployment

e.g.

```bash
# just deploy {{service}} {{environment}} {{tag}}
just deploy concepts-api production latest
```

## uv workspaces

This repo makes use of [uv workspaces](https://docs.astral.sh/uv/concepts/projects/workspaces/)
which are analgous to our [microservices](https://martinfowler.com/articles/microservices.html).

### adding a workspace / microservice

- add your service to the [tool.uv.workspace.members in pyproject.toml](./pyproject.toml#L8)
- add the service to the relevant [.github/workflows](./.github/workflows/)
- add it to the [.trunk/configs/pyrightconfig.json](.trunk/configs/pyrightconfig.json)
