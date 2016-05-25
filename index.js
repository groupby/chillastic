const cluster = require('cluster');
const path    = require('path');
const Master  = require('./app/master');
const Worker  = require('./app/worker');
const stdio   = require('stdio');
const utils   = require('./config/utils');
const log     = require('./config').log;

const options = stdio.getopt({
  concurrency:     {
    key:         'c',
    args:        1,
    default:     1,
    description: 'Max number of threads (default 1, max = # of CPUs)'
  },
  source:          {
    args:      1,
    mandatory: true
  },
  dest:            {
    args:      1,
    mandatory: true
  },
  indices:         {
    key:         'i',
    args:        1,
    description: 'Names of indices to copy configuration (settings, mappings, alias, warmers)'
  },
  data:            {
    key:         'd',
    args:        1,
    description: 'Names of indices from which to copy data'
  },
  templates:       {
    key:         't',
    args:        1,
    description: 'Names of templates to copy'
  },
  indexComparator: {
    args:        1,
    description: 'Module for sorting/prioritizing indices during data transfer'
  },
  indexFilter:     {
    args:        1,
    description: 'Module or regex for including only specific indices in data transfer'
  },
  typeFilter:      {
    args:        1,
    description: 'Module or regex for including only specific types in data transfer'
  },
  mutators:        {
    args:        1,
    description: 'Path to mutator modules'
  }
});

// TODO: Pass in ignoreCompleted

if (options.h) {
  options.printHelp();
  process.exit(1);
}

if (path.extname(options.indexComparator) === '.js') {
  options.indexComparator = utils.parsePath(options.indexComparator);
}

if (path.extname(options.indexFilter) === '.js') {
  options.indexFilter = utils.parsePath(options.indexFilter);
}

if (path.extname(options.typeFilter) === '.js') {
  options.typeFilter = utils.parsePath(options.typeFilter);
}

if (utils.isNonZeroString(options.mutators)) {
  options.mutators = utils.parsePath(options.mutators);
}

const params = {
  concurrency:     options.concurrency,
  indices:         options.indices,
  data:            options.data,
  templates:       options.templates,
  indexComparator: options.indexComparator,
  indexFilter:     options.indexFilter,
  typeFilter:      options.typeFilter,
  mutators:        options.mutators
};

if (cluster.isMaster) {
  log.info('Started with options: ', params);
  const master = new Master(options.source, options.dest);
  master.start(params);
} else {
  const worker = new Worker(options.source, options.dest, params.mutators);
  worker.start(true);
}