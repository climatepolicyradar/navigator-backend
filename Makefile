include .env

ifdef CI
    COMPOSE_FILE = docker-compose.yml
    COMPOSE_CMD = docker compose
else
    COMPOSE_FILE = docker-compose.dev.yml
    COMPOSE_CMD = docker compose -f docker-compose.dev.yml
endif

include ./makefile-local.defs
include ./makefile-docker.defs


check:
	trunk fmt
	trunk check
