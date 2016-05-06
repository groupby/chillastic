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

### CLI Options
There are a bunch of options, I may move this to a config file eventually.

```
USAGE: node index.js [OPTION1] [OPTION2]... arg1 arg2...
The following options are supported:
  -c, --concurrency <ARG1> 	Max number of threads (default 1, max = # of CPUs) ("1" by default)
  --source <ARG1>          	 (mandatory) Source elasticsearch
  --dest <ARG1>            	 (mandatory) Destination elasticsearch
  -i, --indices <ARG1>     	Names of indices to copy configuration (settings, mappings, alias, warmers)
  -d, --data <ARG1>        	Names of indices from which to copy data
  -t, --templates <ARG1>   	Names of templates to copy
  --indexComparator <ARG1> 	Module for sorting/prioritizing indices during data transfer
  --indexFilter <ARG1>     	Module or regex for including only specific indices in data transfer
  --typeFilter <ARG1>      	Module or regex for including only specific types in data transfer
  --mutators <ARG1>        	Path to mutator modules

```

For options `-i -d -t` the names are taken in the typical elasticsearch multi-index format with wildcarding. Found here: https://www.elastic.co/guide/en/elasticsearch/guide/current/multi-index-multi-type.html

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

### How it works

So what does all this do?

Operations are performed in the following order. None of these steps are mandatory and are only executed if the arguments are provided.

1. Index configurations are run through any relevant mutators and transferred
1. Templates are run through any relevant mutators and transferred
1. Find indices for data transfer based on names provided, then filter and sort those indices.
1. Find all types for each of those indices and filter as needed.
1. A list of pending jobs is created in redis. Each job consists of a index and type, prioritized based on the sorting function provided
1. The requested number of workers are created (1 to # of CPUs)
1. Each worker removes a job from the queue, and then adds it to the completed set once it's been completed with no errors.

If you are forced to stop and restart the process, as long as the completed jobs are left in redis they will not be reprocessed.

### Error Handling
Any errors while transferring the index configurations or templates will halt the process.

If an `es_rejected_execution_exception` is detected during data transfer, those records are retried after a random sleep as this only indicates the target is overwhelmed by input. Any other type of error during data transfer results in the entire job failing and being re-added to the end of the job queue to be tried again later.

### Future features

- Dry run mode
- Allow data transfers to be sub-divided based on range queries to a specific field (eg split by a data field)
- Automatic scaling based on search response time from the source, and errors during writes to destination to maximize throughput
- Allow multiple transfer nodes to be brought up and coordinated via redis.
- Allow use of flat file for coordination instead of redis
