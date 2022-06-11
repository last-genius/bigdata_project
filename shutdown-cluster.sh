#!/bin/bash

docker rm -f zookeeper-server kafka-server cassandra-server
docker network rm project-network
