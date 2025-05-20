#!/bin/bash

# filepath: /Users/level3/spotify-mlops-pipeline/setup_docker_environment.sh

set -e  # Exit immediately if a command exits with a non-zero status

echo "Stopping all running containers..."
docker stop $(docker ps -q) || true

echo "Removing all containers..."
docker rm $(docker ps -aq) || true

echo "Removing all Docker images..."
docker rmi $(docker images -q) --force || true

echo "Removing all Docker volumes..."
docker volume rm $(docker volume ls -q) || true

echo "Pruning unused Docker networks..."
docker network prune -f || true

echo "Building and starting Docker containers..."
docker-compose down --volumes --remove-orphans
docker-compose up --build -d

echo "Docker environment setup complete!"
docker ps