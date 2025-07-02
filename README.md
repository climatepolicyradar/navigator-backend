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
