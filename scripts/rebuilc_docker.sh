#!/bin/bash

# filepath: /Users/level3/spotify-mlops-pipeline/clean_and_run_docker.sh

set -e  # Exit immediately if a command exits with a non-zero status

echo "Stopping all running containers..."
docker stop $(docker ps -q) || true

echo "Removing all containers..."
docker rm $(docker ps -aq) || true

echo "Removing all Docker images..."
docker rmi $(docker images -q) --force || true

# echo "Removing all Docker volumes..."
# docker volume rm $(docker volume ls -q) || true

echo "Pruning unused Docker networks..."
docker network prune -f || true

echo "Building and starting the 'spotify_base' module..."
docker-compose up --build -d spotify_base

echo "Waiting for 'spotify_base' to become healthy..."
while [ "$(docker inspect -f '{{.State.Health.Status}}' spotify_base)" != "healthy" ]; do
  echo "Waiting for 'spotify_base'..."
  sleep 5
done

echo "'spotify_base' is healthy. Starting other services..."
docker-compose up --build -d

echo "All services are up and running!"
docker ps