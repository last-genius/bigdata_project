#!/bin/bash

docker rm -f zookeeper-server kafka-server 
docker network rm project-network
