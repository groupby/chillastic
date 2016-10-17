# chillastic
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/636e4a8ac9bd43fab11f33e83061044e)](https://www.codacy.com/app/GroupByInc/chillastic?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=groupby/chillastic&amp;utm_campaign=Badge_Grade) [![Coverage Status](https://coveralls.io/repos/github/groupby/chillastic/badge.svg?branch=master)](https://coveralls.io/github/groupby/chillastic?branch=master) [![Circle CI](https://circleci.com/gh/groupby/chillastic.svg?style=svg)](https://circleci.com/gh/groupby/chillastic)

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

That's great, but it's time to do some work.
 
First we'll define a simple mutator like this: 
```javascript
const TARGET_INDICES_REGEX = /^log_data_v1/;
const NEW_INDEX_NAME = 'log_data_v2';

module.exports = {
  type:      'data',
  predicate: function (doc) {
    return TARGET_INDICES_REGEX.test(doc._index);
  },
  mutate: function (doc) {
    doc._index = doc._index.replace(TARGET_INDICES_REGEX, NEW_INDEX_NAME);

    return doc;
  }
};
```

We'll save that to mutator.js, and then send that to the API:
```bash
curl localhost:9605/mutators/someNamespace/ourMutator -H 'Content-type: text/plain' --data-binary '@mutator.js'
```

Define the task to use the mutator we just sent:
```
{
  "source": {
    "host": "localhost",
    "port": 9200
  },
  "destination": {
    "host": "localhost:9201",
    "port": 9201
  },
  "transfer": {
    "documents": {
      "fromIndices": "log_data_v1*"
    }
  },
  "mutators": {
    "actions": [
      {
        "namespace": "someNamespace",
        "id": "ourMutator"
      }
    ]
  }
}
```

You can push a task into the system using the API:
```
curl -XPOST localhost:9605/tasks/newtask -d '{"source":{"host":"localhost:9200","apiVersion":"1.4"},"destination":{"host":"localhost:9201","apiVersion":"2.2"},"transfer":{"documents":{"fromIndices":"*"}},"mutators":{"path":"path/to/mutators"}}'
```

This task will be split into subtasks, one for each combination of index and type. The workers will then transfer all the documents associated with a specific subtask from one elasticsearch to the other.

### More docs to come
