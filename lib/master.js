import cluster from 'cluster';
import _ from 'lodash';
import moment from 'moment';
require("moment-duration-format");
import Promise from 'bluebird';

import Transfer from './transfer';
import Manager from './manager';
import createEsClient from '../config/elasticsearch.js'
import config from '../config';
var log = config.log;

var DATE_FORMAT = 'YYYY-MM-DD';
var DATE_REGEX  = /[0-9]{4}-[0-9]{2}-[0-9]{2}/;

var isNonZeroString = (input) => {
  return _.isString(input) && input.length > 0;
};

var source = null;
var dest   = null;

var transfer          = null;
var manager           = null;
var workers           = [];
var workerProgress    = {};
var completedCallback = null;

var progressInterval = null;
var startTime        = null;
var totalJobs        = 0;

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
      return manager.getBacklogCount().then((backlogCount)=> {
        totalJobs += backlogCount;
        return manager.getCompletedCount();
      }).then((completedCount)=> {
        totalJobs += completedCount;
        return startWorkers(params);
      });
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
      }
    });

    workers.push(worker);
  }

  startTime        = moment();
  progressInterval = setInterval(printProgress, 10 * 1000);
};

/**
 * Print a summary of the current state
 */
var printProgress = ()=> {
  manager.getCompletedCount().then((completedCount)=> {

    let overallProgress = (completedCount / totalJobs) * 100;

    let currentTime   = moment();
    let elapsedMsec   = currentTime.valueOf() - startTime.valueOf();
    let projectedMsec = elapsedMsec / (completedCount / totalJobs);

    let projectedTime   = moment(projectedMsec);
    let elapsedDuration = moment.duration(elapsedMsec);

    log.info();
    log.info('**********************************');
    log.info('Worker Status:');
    _.forEach(workerProgress, (status, id)=> {
      let progress = (status.transferred / status.total) * 100;
      log.info('Worker: ' + id + ' Progress: ' + progress.toFixed(2) + '% Job: ' + status.job.index + '/' + status.job.type);
    });
    log.info('----------------------------------');
    log.info('Overall Status:');
    log.info('Progress: ' + overallProgress.toFixed(2) + '%');
    log.info('Started:' + startTime.format('MMM D HH:mm:ss'));
    log.info('Elapsed: ' + elapsedDuration.format("d[d] h:mm:ss"));
    log.info();
    log.info('Estimated Completion: ' + projectedTime.format('MMM D HH:mm:ss'));
    log.info('**********************************');
    log.info();
  });
};

var updateWorkerProgress = (status, workerId)=> {
  workerProgress[workerId] = status;
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