import cluster from 'cluster';
import _ from 'lodash';
import moment from 'moment';
require("moment-duration-format");
import Promise from 'bluebird';

import utils from '../config/utils';
import Transfer from './transfer';
import Manager from './manager';
import createEsClient from '../config/elasticsearch.js'
import config from '../config';
var log = config.log;

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
var masterPid        = null;

/**
 * Master constructor
 *
 * @param sourceUrl
 * @param destUrl
 * @constructor
 */
var Master = function (sourceUrl, destUrl) {
  var self = this;

  if (!utils.isNonZeroString(sourceUrl)) {
    throw new Error('source must be non-zero string');
  }

  if (!utils.isNonZeroString(destUrl)) {
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

  masterPid = process.pid;

  if (utils.isNonZeroString(params.mutators)) {
    transfer.loadMutators(params.mutators);
  }

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
    if (utils.isNonZeroString(params.data)) {
      return manager.getBacklogCount().then((backlogCount)=> {
        totalJobs += backlogCount;
        return manager.getCompletedCount();
      }).then((completedCount)=> {
        totalJobs += completedCount;
        return startWorkers(params);
      });
    } else {
      log.info('===========================');
      log.info('Complete!');
      log.info('===========================');
      process.exit();
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
  if (params.indices && !utils.isNonZeroString(params.indices)) {
    throw new Error('if provided, indices must be a index name or multi-index query');
  }

  if (params.templates && !utils.isNonZeroString(params.templates)) {
    throw new Error('if provided, templates must be a template name or multi-template query');
  }

  if (params.data && !utils.isNonZeroString(params.data)) {
    throw new Error('if provided, data must be an index name or multi-index query of the data to be transferred');
  }

  params.concurrency = parseInt(params.concurrency);

  if (!_.isNumber(params.concurrency) || _.isNaN(params.concurrency) || params.concurrency < 1) {
    throw new Error('concurrency must be a number gte 1');
  }
};

var ifStringProvided = (argument, promise)=> {
  if (utils.isNonZeroString(argument)) {
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
      log.info('===========================');
      log.info('Complete!');
      log.info('===========================');
      printProgress();
      process.exit();
    }
  });

  for (var i = 0; i < numWorkers; i++) {
    let worker = cluster.fork();

    worker.on('message', (message)=> {
      if (message.message) {
        log.info('Worker ' + message.pid + ': [' + message.level + '] : ' + message.message);
      } else {
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
    let projectedMsec = currentTime.valueOf() + (elapsedMsec / (completedCount / totalJobs));

    let projectedTime   = moment(projectedMsec);
    let elapsedDuration = moment.duration(elapsedMsec);

    log.info('-');
    log.info('**********************************');
    log.info('Worker Status:');
    _.forEach(workerProgress, (status, id)=> {
      let progress = (status.transferred / status.total) * 100;
      log.info('Worker: ' + id + ' Progress: ' + progress.toFixed(2) + '% Job: ' + status.job.index + '/' + status.job.type);
    });
    log.info('----------------------------------');
    log.info('Overall Status:');
    log.info('Master:           ' + masterPid);
    log.info('Total Docs:       ' + totalJobs);
    log.info('Transferred Docs: ' + completedCount);
    log.info('Progress:         ' + overallProgress.toFixed(2) + '%');
    log.info('Started:          ' + startTime.format('MMM D HH:mm:ss'));
    log.info('Elapsed:          ' + elapsedDuration.format("d[d] h:mm:ss"));
    let completionDate = projectedTime.isValid() ? projectedTime.format('MMM D HH:mm:ss') : 'unknown';
    log.info('Estimated Completion: ' + completionDate);
    log.info('**********************************');
    log.info('-');
  });
};

var updateWorkerProgress = (status, workerId)=> {
  workerProgress[workerId] = status;

  if (status.transferred === 0) {
    log.info('Worker: ' + workerId + ' Starting Job: ' + status.job.index + '/' + status.job.type);
  }
};

export default Master;