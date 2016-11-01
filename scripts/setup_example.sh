#!/usr/bin/env bash
set -e

./docker_dependencies.sh
echo "Wait for es startup"
sleep 10
python ../../elasticsearch-test-data/es_test_data.py --es_url=http://localhost:9200 --index_name=log_data_2016-10-01 --count=100000
python ../../elasticsearch-test-data/es_test_data.py --es_url=http://localhost:9200 --index_name=log_data_2016-10-02 --count=100000
python ../../elasticsearch-test-data/es_test_data.py --es_url=http://localhost:9200 --index_name=log_data_2016-10-03 --count=100000
python ../../elasticsearch-test-data/es_test_data.py --es_url=http://localhost:9200 --index_name=log_data_2016-11-01 --count=100000
python ../../elasticsearch-test-data/es_test_data.py --es_url=http://localhost:9200 --index_name=log_data_2016-11-02 --count=100000
python ../../elasticsearch-test-data/es_test_data.py --es_url=http://localhost:9200 --index_name=log_data_2016-12-01 --count=100000
node ../example.js