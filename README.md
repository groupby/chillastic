# chillastic
[![Circle CI](https://circleci.com/gh/groupby/chillastic.svg?style=svg)](https://circleci.com/gh/groupby/chillastic)

Reindex multiple elasticsearch indices, save your progress, mutate your data in-flight.

### Requirements
Currently chillastic relies on redis to store the current state of the transfer and any pending jobs.

The easiest way to setup if you have docker installed is to run the following:
```
docker run -it -d -p 6379:6379 --name redis redis:3
```

Otherwise, point the application at another redis install using the `REDIS_HOST` environment variable.

### Steps
Right now it's a bit manual, but it will improve shortly.

```
clone repo
npm run build
cd build
node index.js <args>
```

### Examples

To run these examples, either change the source and destination in the command, or if you have docker installed, run `./scripts/docker_dependencies.sh`.

To create some test data, you can use the elasticsearch data generator found here: https://github.com/oliver006/elasticsearch-test-data

Generate the test data using that generator with:
```
python es_test_data.py --es_url=http://localhost:9200 --index-name=testdata-2015-05-21 --count=100000
python es_test_data.py --es_url=http://localhost:9200 --index-name=testdata-2015-03-01 --count=100000
python es_test_data.py --es_url=http://localhost:9200 --index-name=testdata-2015-05-05 --count=100000
python es_test_data.py --es_url=http://localhost:9200 --index-name=not-testdata --count=100000
```

Transfer all index configurations, and all indices and types from one elasticsearch to another.

```
node index.js --source localhost:9200 --dest localhost:9201 -i '*' -d '*'
```

Transfer just index data (not mappings, settings, aliases, etc) for indices starting with `testing`.

```
node index.js --source localhost:9200 --dest localhost:9201 -d 'testing*'
```

Transfer just index data, and use the indexDate mutator.

```
node index.js --source localhost:9200 --dest localhost:9201 -d '*' --mutators './examples/mutators/indexDate.js'
```