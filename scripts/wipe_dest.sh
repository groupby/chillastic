#!/usr/bin/env bash
docker rm -f dest_es
docker rm -f redis
docker pull elasticsearch:2.2
docker pull redis:3
docker run -d --name dest_es -p 9201:9200 elasticsearch:2.2
docker run -d --name redis -p 6379:6379 redis:3