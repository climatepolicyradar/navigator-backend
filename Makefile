include .env
include ./makefile-local.defs
include ./makefile-docker.defs

check:
	trunk fmt
	trunk check
