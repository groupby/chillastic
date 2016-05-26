const cluster = require('cluster');
const _       = require('lodash');
const moment  = require('moment');
require("moment-duration-format");
const Promise = require('bluebird');

const utils             = require('../config/utils');
const Transfer          = require('./transfer');
const Manager           = require('./manager');
const createEsClient    = require('../config/elasticsearch.js');
const createRedisClient = require('../config/redis');
const config            = require('../config');
const log               = config.log;

let source = null;
let dest   = null;

let transfer          = null;
let manager           = null;
const workers         = [];
const workerProgress  = {};
let completedCallback = null;

let startTime = null;
let totalJobs = 0;
let masterPid = null;

/**
 * Master constructor
 *
 * @param sourceConfig
 * @param destConfig
 * @param redisConfig
 * @constructor
 */
const Master = function (sourceConfig, destConfig, redisConfig) {
  const self = this;

  self.source = createEsClient(sourceConfig.host, sourceConfig.apiVersion);
  source      = self.source;
  self.dest   = createEsClient(destConfig.host, sourceConfig.apiVersion);
  dest        = self.dest;
  transfer    = new Transfer(source, dest);
  manager     = new Manager(source, createRedisClient(redisConfig.hostname, redisConfig.port));

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
const start = (params)=> {
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
const validate = (params)=> {
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

const ifStringProvided = (argument, promise)=> {
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
const startWorkers = (params)=> {
  log.info('starting workers');

  const numCPUs    = require('os').cpus().length;
  const numWorkers = (params.concurrency < numCPUs) ? params.concurrency : numCPUs;

  let exited = 0;
  cluster.on('exit', (worker, code, signal) => {
    if (signal) {
      log.fatal(`worker was killed by signal: ${signal}`);
    } else if (code !== 0) {
      log.fatal(`worker exited with error code: ${code}`);
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

  for (let i = 0; i < numWorkers; i++) {
    const worker = cluster.fork({WORKER_CONFIG: JSON.stringify(params)});

    worker.on('message', (message)=> {
      if (message.message) {
        log.info(`Worker ${message.pid}: [${message.level}] : ${message.message}`);
      } else {
        updateWorkerProgress(message, message.pid);
      }
    });

    workers.push(worker);
  }

  startTime        = moment();
  setInterval(printProgress, 10 * 1000);
};

/**
 * Print a summary of the current state
 */
const printProgress = ()=> {
  manager.getCompletedCount().then((completedCount)=> {

    const overallProgress = (completedCount / totalJobs) * 100;

    const currentTime   = moment();
    const elapsedMsec   = currentTime.valueOf() - startTime.valueOf();
    const projectedMsec = currentTime.valueOf() + (elapsedMsec / (completedCount / totalJobs));

    const projectedTime   = moment(projectedMsec);
    const elapsedDuration = moment.duration(elapsedMsec);

    log.info('-');
    log.info('**********************************');
    log.info('Worker Status:');
    _.forEach(workerProgress, (status, id)=> {
      const progress = (status.transferred / status.total) * 100;
      log.info(`Worker: ${id} Progress: ${progress.toFixed(2)}% Job: ${status.job.index}/${status.job.type}`);
    });
    log.info('----------------------------------');
    log.info('Overall Status:');
    log.info(`Master:           ${masterPid}`);
    log.info(`Total Docs:       ${totalJobs}`);
    log.info(`Transferred Docs: ${completedCount}`);
    log.info(`Progress:         ${overallProgress.toFixed(2)}%`);
    log.info(`Started:          ${startTime.format('MMM D HH:mm:ss')}`);
    log.info(`Elapsed:          ${elapsedDuration.format("d[d] h:mm:ss")}`);
    const completionDate = projectedTime.isValid() ? projectedTime.format('MMM D HH:mm:ss') : 'unknown';
    log.info(`Estimated Completion: ${completionDate}`);
    log.info('**********************************');
    log.info('-');
  });
};

const updateWorkerProgress = (status, workerId)=> {
  workerProgress[workerId] = status;

  if (status.transferred === 0) {
    log.info(`Worker: ${workerId} Starting Job: ${status.job.index}/${status.job.type}`);
  }
};

module.exports = Master;