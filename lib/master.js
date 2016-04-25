const cluster = require('cluster');
import _ from 'lodash';
import moment from 'moment';
import ProgressBar from 'progress';

import Manager from './manager';
import createEsClient from '../config/elasticsearch.js'
import config from '../config';
var log = config.log;

import Transfer from './transfer';
import Promise from 'bluebird';

var DATE_FORMAT = 'YYYY-MM-DD';
var DATE_REGEX  = /[0-9]{4}-[0-9]{2}-[0-9]{2}/;

var isNonZeroString = (input) => {
  return _.isString(input) && input.length > 0;
};

var source = null;
var dest   = null;

var overallBar        = null;
var transfer          = null;
var manager           = null;
var workers           = [];
var workerProgress    = {};
var completedCallback = null;

/**
 * Master constructor
 *
 * @param sourceUrl
 * @param destUrl
 * @constructor
 */
var Master = function (sourceUrl, destUrl) {
  var self = this;

  if (!isNonZeroString(sourceUrl)) {
    throw new Error('source must be non-zero string');
  }

  if (!isNonZeroString(destUrl)) {
    throw new Error('dest must be non-zero string');
  }

  self.source = createEsClient(sourceUrl, '1.4');
  source      = self.source;
  self.dest   = createEsClient(destUrl, '2.2');
  dest        = self.dest;
  transfer    = new Transfer(source, dest);
  manager     = new Manager(source);

  self.setCompletedCallback = (callback)=> {
    completedCallback = callback;
  };

  self.start = start;
};

/**
 * Copy index configurations and templates as needed, then start workers to copy data
 *
 * @param params
 * @returns {Promise.<TResult>}
 */
var start = (params)=> {
  validate(params);

  return ifStringProvided(params.indices, transfer.transferIndices).then(()=> {
    return ifStringProvided(params.templates, transfer.transferTemplates);
  }).then(()=> {
    return ifStringProvided(params.indexFilter, manager.setIndexFilter);
  }).then(()=> {
    return ifStringProvided(params.indexComparator, manager.setIndexComparator);
  }).then(()=> {
    return ifStringProvided(params.typeFilter, manager.setTypeFilter);
  }).then(()=> {
    return ifStringProvided(params.data, manager.initialize);
  }).then(()=> {
    if (isNonZeroString(params.data)) {
      return startWorkers(params);
    } else {
      return Promise.resolve();
    }
  });
};

/**
 * Validate parameter input
 *
 * @param params
 */
var validate = (params)=> {
  if (params.indices && !isNonZeroString(params.indices)) {
    throw new Error('if provided, indices must be a index name or multi-index query');
  }

  if (params.templates && !isNonZeroString(params.templates)) {
    throw new Error('if provided, templates must be a template name or multi-template query');
  }

  if (params.data && !isNonZeroString(params.data)) {
    throw new Error('if provided, data must be an index name or multi-index query of the data to be transferred');
  }

  params.concurrency = parseInt(params.concurrency);

  if (!_.isNumber(params.concurrency) || _.isNaN(params.concurrency) || params.concurrency < 1) {
    throw new Error('concurrency must be a number gte 1');
  }
};

var ifStringProvided = (argument, promise)=> {
  if (isNonZeroString(argument)) {
    return promise(argument);
  } else {
    return Promise.resolve();
  }
};

/**
 * Start worker processes to consume job queue
 *
 * @param params
 */
var startWorkers = (params)=> {
  log.info('starting workers');

  const numCPUs    = require('os').cpus().length;
  const numWorkers = (params.concurrency < numCPUs) ? params.concurrency : numCPUs;

  let exited = 0;
  cluster.on('exit', function (worker, code, signal) {
    if (signal) {
      log.fatal('worker was killed by signal: ' + signal);
      console.log('worker was killed by signal: ' + signal);
    } else if (code !== 0) {
      log.fatal('worker exited with error code: ' + code);
      console.log('worker exited with error code: ' + code);
    }

    exited++;
    if (exited >= numWorkers) {
      if (completedCallback) completedCallback();
      process.exit();
    }
  });

  for (var i = 0; i < numWorkers; i++) {
    log.info('master', cluster.isMaster);
    let worker = cluster.fork();

    worker.on('message', (message)=> {
      if (message.message) {
        log.info('Worker ' + message.pid + ': [' + message.level + '] : ' + message.message);
      } else if (message.transferred) {
        updateWorkerProgress(message, message.pid);
        updateOverallProgress();
      }
    });

    workers.push(worker);
  }
};

var updateWorkerProgress = (status, workerId)=> {
  let bar = workerProgress[workerId];

  // if (!bar || status.transferred === 0) {
  //   if (bar) {
  //     bar.terminate();
  //   }
  //
  //   const barConfig = {
  //     total:  parseInt(status.total),
  //     format: '' + workerId + ' job: ' + status.job.index + '/' + status.job.type + ' [:bar] :percent :etas'
  //   };
  //
  //   bar                      = new ProgressBar(barConfig);
  //   workerProgress[workerId] = bar;
  //
  //   if (status.transferred > 0) {
  //     bar.tick(status.transferred);
  //   }
  // } else {
  //   bar.tick(status.tick);
  //   if (bar.complete) {
  //     bar                      = null;
  //     workerProgress[workerId] = null;
  //   }
  // }
};

/**
 * Update overall progress bar
 */
var updateOverallProgress = ()=> {
  manager.getBacklogCount().then((backlogCount)=> {
    return manager.getCompletedCount().then((completedCount)=> {
      let total = manager.getTotalCount();

      if (!overallBar) {
        overallBar = new ProgressBar('overall: [:bar] :percent :etas', {total: total});
      }

      overallBar.update(completedCount/total);
    });
  })
};


var indicesComparator = (a, b)=> {
  if (DATE_REGEX.test(a.index) && DATE_REGEX.test(b.index)) {
    var aDate = moment(a.index.match(DATE_REGEX), DATE_FORMAT);
    var bDate = moment(b.index.match(DATE_REGEX), DATE_FORMAT);

    // Sort descending date
    var diff = bDate.valueOf() - aDate.valueOf();

    // Sort alphabetically if date is identical
    return (diff === 0) ? a.index.localeCompare(b.index) : diff;

  } else if (DATE_REGEX.test(a.index)) {
    return 1;
  } else if (DATE_REGEX.test(b.index)) {
    return -1;
  } else {
    return a.index.localeCompare(b.index);
  }
};

export default Master;