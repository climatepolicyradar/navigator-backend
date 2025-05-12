#!/bin/bash

# Check if container name is provided
if [[ $# -eq 0 ]]; then
	echo "Usage: $0 <container_name>"
	exit 1
fi

container_name=$1

until [[ "$(docker inspect --format='{{.State.Health.Status}}' "$(docker compose ps -q "${container_name}")")" == "healthy" ]]; do
	sleep 1
done
