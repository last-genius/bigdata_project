#!/bin/bash

docker build -f Dockerfile.read -t kafka_read .
docker run --network project-network --rm kafka_read
