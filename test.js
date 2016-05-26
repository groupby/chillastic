var Chillastic = require('./index');

let configuration = {
  source:      {
    host:       'localhost:9200',
    apiVersion: '1.4'
  },
  destination: {
    host:       'localhost:9201',
    apiVersion: '2.2'
  },
  redis:       {
    hostname: 'localhost',
    port:     6379
  },
  concurrency: 3,
  indices:     '*',
  data:        '*'
};

Chillastic(configuration);