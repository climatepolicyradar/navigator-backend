# Development using docker

Run the stack with

```shell
docker-compose up
```

or

```shell
make start
```

## Rebuilding containers

```shell
docker-compose build
```

## Restarting containers

```shell
docker-compose restart
```

## Bringing containers down

```shell
docker-compose down
```

## Logging

```shell
docker-compose logs
```

Or for a specific service:

```shell
docker-compose logs -f name_of_service # frontend|backend|db
```
