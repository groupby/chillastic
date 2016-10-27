#!/usr/bin/env bash
set -e

./docker_dependencies.sh
echo "Wait for es startup"
sleep 10
python ../../elasticsearch-test-data/es_test_data.py --es_url=http://localhost:9200 --index_name=first --count=10000
python ../../elasticsearch-test-data/es_test_data.py --es_url=http://localhost:9200 --index_name=second --count=10000
python ../../elasticsearch-test-data/es_test_data.py --es_url=http://localhost:9200 --index_name=third --count=10000
python ../../elasticsearch-test-data/es_test_data.py --es_url=http://localhost:9200 --index_name=fourth --count=10000
python ../../elasticsearch-test-data/es_test_data.py --es_url=http://localhost:9200 --index_name=fifth --count=10000
node ../example.js