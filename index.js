const cluster   = require('cluster');
const path      = require('path');
const Master    = require('./app/master');
const Worker    = require('./app/worker');
const utils     = require('./config/utils');
const log       = require('./config').log;
const inspector = require('schema-inspector');

const SCHEMA = {
  type:       'object',
  properties: {
    source:          {
      type:       'object',
      properties: {
        host:       {
          type:      'string',
          minLength: 3
        },
        apiVersion: {
          type:      'string',
          minLength: 3
        }
      }
    },
    destination:     {
      type:       'object',
      properties: {
        host:       {
          type:      'string',
          minLength: 3
        },
        apiVersion: {
          type:      'string',
          minLength: 3
        }
      }
    },
    redis:           {
      type:     'object',
      hostname: {
        type:      'string',
        minLength: 7,
        maxLength: 15
      },
      port:     {
        type: 'integer',
        gt:   0,
        lte:  65535
      }
    },
    concurrency:     {
      type: 'integer',
      gte:  1,
      def:  1
    },
    indices:         {
      type: 'string'
    },
    data:            {
      type: 'string'
    },
    templates:       {
      type: 'string'
    },
    indexComparator: {
      type: 'string'
    },
    indexFilter:     {
      type: 'string'
    },
    typeFilter:      {
      type: 'string'
    },
    mutators:        {
      type: 'string'
    }
  }
};

const create = (configuration)=> {
  inspector.sanitize(SCHEMA, configuration);
  const result = inspector.validate(SCHEMA, configuration);

  if (!result.valid) {
    throw new Error(result.format());
  }

  if (path.extname(configuration.indexComparator) === '.js') {
    configuration.indexComparator = utils.parsePath(configuration.indexComparator);
  }

  if (path.extname(configuration.indexFilter) === '.js') {
    configuration.indexFilter = utils.parsePath(configuration.indexFilter);
  }

  if (path.extname(configuration.typeFilter) === '.js') {
    configuration.typeFilter = utils.parsePath(configuration.typeFilter);
  }

  if (utils.isNonZeroString(configuration.mutators)) {
    configuration.mutators = utils.parsePath(configuration.mutators);
  }

  if (cluster.isMaster) {
    log.info('Started with configuration. ', configuration);
    const master = new Master(configuration.source, configuration.destination, configuration.redis);
    master.start(configuration);
  } else {
    const workerConfig = JSON.parse(process.env.WORKER_CONFIG);
    const worker = new Worker(workerConfig.source, workerConfig.destination, workerConfig.redis, workerConfig.mutators);
    worker.start(true);
  }
};

module.exports = create;

