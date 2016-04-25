import cluster from 'cluster';
import Master from './lib/master';
import Worker from './lib/worker';
import stdio from 'stdio';

var options = stdio.getopt({
  concurrency: {
    key:         'c',
    args:        1,
    default:     1,
    description: 'Max number of threads (default 1, max = # of CPUs)'
  },
  source:      {
    args:      1,
    mandatory: true
  },
  dest:        {
    args:      1,
    mandatory: true
  },
  indices:     {
    key:         'i',
    args:        1,
    description: 'Names of indices to copy configuration (settings, mappings, alias, warmers)'
  },
  data:        {
    key:         'd',
    args:        1,
    description: 'Names of indices from which to copy data'
  },
  templates:   {
    key:         't',
    args:        1,
    description: 'Names of templates to copy'
  },
  indexFilter: {
    args:        1,
    description: 'Regex for including only specific indices in data transfer'
  },
  typeFilter:  {
    args:        1,
    description: 'Regex for including only specific types in data transfer'
  }
});

if (options.h) {
  console.log('options.args.length: ' + options.args.length);
  console.log(JSON.stringify(options, null, 2));
  options.printHelp();
  process.exit(1);
}

var params = {
  concurrency: options.concurrency,
  indices:     options.indices,
  data:        options.data,
  templates:   options.templates,
  indexFilter: options.indexFilter,
  typeFilter:  options.typeFilter
};

if (cluster.isMaster) {
  console.log(JSON.stringify(options, null, 2));

  var master = new Master(options.source, options.dest);
  master.start(params);
} else {
  var worker = new Worker(options.source, options.dest);

  worker.start(true);
}