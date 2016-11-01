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
```json
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
```bash
curl -XPOST localhost:9605/tasks/newtask -d '{"source":{"host":"localhost","port":9200},"destination":{"host":"localhost:9201","port":9201},"transfer":{"documents":{"fromIndices":"log_data_v1*"}},"mutators":{"actions":[{"namespace":"someNamespace","id":"ourMutator"}]}}'
```

This task will be split into subtasks, one for each combination of index and type. The workers will then transfer all the documents associated with a specific subtask from one elasticsearch to the other.

### Tasks
A task defines the work to be done during reindexing and has the following possible fields:

```javascript
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
      "flushSize": 25000,               // Max number of docs in a bulk operation
      "fromIndices": "log_data_v1*",    // Any index names that match this will have their docs transferred
      "filters": {
        "actions": [
          {
            "namespace": "someNamespace",
            "id": "ourFilter",
            "arguments": {}
          }
        ],
        "arguments": {}
      }
    },
    "indices": {
      "name": "*log_data*",     // Any index names that match this will have their settings, mappings, aliases copied
      "templates": "*log_data*" // Any template names that match this will be copied
    }
  },
  "mutators": {
    "actions": [
      {
        "namespace": "someNamespace",
        "id": "ourMutator"
      }
    ],
    "arguments": {}
  }
}
```

### Mutators
Mutators can be of type 'data', 'index', or 'template' and apply to documents, index configurations, and templates respectively.

They are defined as javascript modules and loaded by POSTing them to the mutators/ API endpoint.

```javascript
// The libraries for 'moment' and 'lodash' are available inside the mutator definition
const moment = require('moment');

const OLD_DATE_FORMAT = 'YYYY-MM-DD';
const OLD_DATE_REGEX  = /[0-9]{4}-[0-9]{2}-[0-9]{2}/;
const NEW_DATE_FORMAT = 'YYYY-MM';

module.exports = {
  /**
   * Type of mutator
   */
  type:      'data',
  /**
   * The predicate function is called for every target document
   * @param doc - The document to be checked against the predicate
   * @param arguments - The task-specific arguments object
   * @returns {boolean}
   */
  predicate: function (doc, arguments) {
    return OLD_DATE_REGEX.test(doc._index);
  },
  /**
   * The mutate function is only called on documents that satisfy the predicate
   * @param doc - The document that satisfied the predicate
   * @param arguments - The task-specific arguments object
   * @returns {*}
   */
  mutate: function (doc, arguments) {
    const date = moment(doc._index.match(OLD_DATE_REGEX), OLD_DATE_FORMAT);
    doc._index = doc._index.replace(OLD_DATE_REGEX, date.format(NEW_DATE_FORMAT));

    return doc;
  }
};
```

### Filters
Filters are used prior to document transfer to exclude specific types or indicies prior to the task starting. While a mutator could be used for this by returning null on specific documents, a filter has the advantage of removing entire indicies and types prior to processing.

Filters are also javascript modules.

```javascript
module.exports = {
  type: 'index',
  /**
   * Any indicies that trigger this predicate will be excluded from transfer
   * @param index - Full index configuration
   * @param arguments - The task-specific arguments object
   */
  predicate: (index, arguments) => index.name === arguments.targetName
};
```


### More docs to come
