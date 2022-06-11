#!/bin/bash

docker build -f Dockerfile.server -t server .
docker run -p 5000:5000 --network project-network --rm server
