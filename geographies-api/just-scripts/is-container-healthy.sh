#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
	echo "Usage: $0 <container_name>"
	exit 1
fi

container_name=$1

# Grab the container ID, exit on failure
container_id=$(docker compose ps -q "${container_name}") ||
	{
		echo "Error: no such container '${container_name}'"
		exit 1
	}

# Loop until the container reports healthy
health_status=$(docker inspect --format='{{.State.Health.Status}}' "${container_id}")
until [[ ${health_status} == "healthy" ]]; do
	sleep 10
done

echo "Container ${container_name} is healthy"
