const _          = require('lodash');
const Chillastic = require('./index'); // Replace with 'require('chillastic')' if you're outside of this repo

const REDIS_HOST = 'localhost';
const REDIS_PORT = 6379;
const CHILL_PORT = _.random(4000, 8000);

const chillastic = Chillastic(REDIS_HOST, REDIS_PORT, CHILL_PORT);

// Start it up!
chillastic.run();

var thing = {
  "source":      {
    "host": "localhost",
    "port": 9200
  },
  "destination": {
    "host": "localhost",
    "port": 9201
  },
  "transfer":    {
    "documents": {
      "fromIndices": "*",
      "filters":     {
        "actions": [
          {
            "namespace": "someNamespace",
            "id":        "indexFilter"
          }
        ]
      }
    }
  },
  "mutators":    {
    "actions": [
      {
        "namespace": "someNamespace",
        "id":        "indexMutator"
      }
    ]
  }
};