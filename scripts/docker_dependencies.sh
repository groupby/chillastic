#!/usr/bin/env bash
docker rm -f source_es
docker rm -f dest_es
docker rm -f redis
docker pull elasticsearch:1.4
docker pull elasticsearch:2.3
docker pull redis:3.2
docker run -d --name source_es -p 9200:9200  elasticsearch:1.4
docker run -d --name dest_es -p 9201:9200 elasticsearch:2.3
docker run -d --name redis -p 6379:6379 redis:3.2