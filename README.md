# chillastic
[![Circle CI](https://circleci.com/gh/groupby/chillastic.svg?style=svg)](https://circleci.com/gh/groupby/chillastic)

Reindex multiple elasticsearch indices, save your progress, mutate your data in-flight.

### How to use it
Install into your project with:
```
npm install --save chillastic
```

Create an instance of it doing something like (also seen in example.js):
```
const Chillastic = require('chillastic');
const _ = require('lodash');

const REDIS_HOST = 'localhost';
const REDIS_PORT = 6379;
const CHILL_PORT = _.random(7000, 10000);

const chillastic = Chillastic(REDIS_HOST, REDIS_PORT, CHILL_PORT);

// Start it up!
chillastic.run();
```

Running the code above will create a single chillastic worker with an API on a random port between 7000-10000. You should see something like the following on console out:
```
13:19:28.519Z  WARN chillastic : 
    Starting with config: {
      "FRAMEWORK_NAME": "chillastic",
      "logLevel": "info",
      "elasticsearch": {
        "logLevel": "warn"
      },
      "redis": {
        "host": "localhost",
        "port": 6379
      },
      "port": 9605
    }
13:19:28.530Z  WARN chillastic : chillastic server listening on port 9605
13:19:28.544Z  INFO chillastic : Starting worker: Rapidskinner Grin
13:19:28.545Z  INFO chillastic : No tasks found, waiting...
13:19:30.548Z  INFO chillastic : No tasks found, waiting...
```

To get the status of the system, and a full list of workers:
```
curl localhost:9605/status

{"manager":"running","workers":{"Rapidskinner Grin":{"status":"waiting for task..."}}}
```

To add another worker, just start another instance pointed at the same redis instance. Then check the status again for a response like:
```
curl localhost:9605/health

{"manager":"running","workers":{"Rapidskinner Grin":{"status":"waiting for task..."},"Windshift Fairy":{"status":"waiting for task..."}}}
```

That's great, but it's time to do some work. A task definition looks something like this:
```
{
  "source": {
    "host": "localhost:9200",
    "apiVersion": "1.4"
  },
  "destination": {
    "host": "localhost:9201",
    "apiVersion": "2.2"
  },
  "transfer": {
    "documents": {
      "fromIndices": "*"
    }
  },
  "mutators": {
    "path": "path/to/mutators"
  }
}
```

You can push a task into the system using the API:
```
curl -XPOST localhost:9605/tasks/newtask -d '{"source":{"host":"localhost:9200","apiVersion":"1.4"},"destination":{"host":"localhost:9201","apiVersion":"2.2"},"transfer":{"documents":{"fromIndices":"*"}},"mutators":{"path":"path/to/mutators"}}'
```

This task will be split into subtasks, one for each combination of index and type. The workers will then transfer all the documents associated with a specific subtask from one elasticsearch to the other.

### More docs to come

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
