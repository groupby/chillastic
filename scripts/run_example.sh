#!/usr/bin/env bash
set -e

curl -XPOST localhost:8080/tasks/example -d '{"source":{"host":"localhost","port":9200}, "destination":{"host":"localhost","port":9201}, "transfer":{"documents":{"fromIndices":"*"}}}'