var Chillastic = require('./index');

let configuration = {
  source:      {
    host: 'localhost:9200',
  },
  destination: {
    host: 'localhost:9201',
  },
  redis:       {
    hostname: 'localhost',
    port:     6379
  },
};