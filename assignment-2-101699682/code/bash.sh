#!/bin/bash

# Building the Docker images as per docker-compose.yml
docker-compose -f docker-compose.yml build

# Bringing down any existing containers with the same configuration, removing volumes
docker-compose -f docker-compose.yml down -v

# Bringing up the containers in detached mode and forcing recreation of containers
docker-compose -f docker-compose.yml up -d --force-recreate
