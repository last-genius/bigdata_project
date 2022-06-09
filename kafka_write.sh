#!/bin/bash

docker build -f Dockerfile.write -t kafka_write .
docker run --network project-network --rm kafka_write
