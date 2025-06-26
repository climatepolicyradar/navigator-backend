include .env

ifdef CI
    COMPOSE_CMD = docker compose
else
    # merges docker-compose.yml and docker-compose.dev.yml for a better UX locally
    # @see ./docker-compose.dev.yml
    # @see https://docs.docker.com/compose/how-tos/multiple-compose-files/merge/
    COMPOSE_CMD = docker compose -f docker-compose.yml -f docker-compose.dev.yml
endif

include ./makefile-local.defs
include ./makefile-docker.defs


check:
	trunk fmt
	trunk check
