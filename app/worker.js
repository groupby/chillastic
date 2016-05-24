const Promise = require('bluebird');
const cluster = require('cluster');

const utils          = require('../config/utils');
const Transfer       = require('./transfer');
const Manager        = require('./manager');
const config         = require('../config');
const createEsClient = require('../config/elasticsearch.js');
const log            = config.log;

let transfer               = null;
let manager                = null;
let overrideProgressUpdate = null;

/**
 * Worker constructor
 *
 * @param sourceUrl
 * @param destUrl
 * @constructor
 */
const Worker = function (sourceUrl, destUrl, mutators) {
  log.info(`worker created: ${process.pid}`);
  const self    = this;
  self.source = createEsClient(sourceUrl, '1.4');
  self.dest   = createEsClient(destUrl, '2.2');

  transfer = new Transfer(self.source, self.dest);
  manager  = new Manager(self.source);

  if (utils.isNonZeroString(mutators)) {
    transfer.loadMutators(mutators);
  }

  self._overrideProgresUpdate = (callback)=> {
    overrideProgressUpdate = callback;
  };

  self.start = (exitOnComplete) => {
    log.info(`worker: ${process.pid} started...`);
    return doJob().then(()=> {
      if (exitOnComplete) {
        process.exit();
      }

      return Promise.resolve();
    });
  }
};

/**
 * Recursively pull jobs from the queue and execute them.
 *
 * NOTE: bluebird promises have special handling to prevent recursive promises from blowing the stack
 *
 * @returns {Promise.<TResult>}
 */
const doJob = ()=> {
  return manager.fetchJob().then((job)=> {
    if (job === null) {
      // log.warn('No more jobs. Complete');
      return Promise.resolve();
    }

    // Announce that a new job is started
    progressUpdate({
      tick:        0,
      transferred: 0,
      total:       job.count,
      job:         job
    });

    // Callback is called every time the bulk queue is flushed
    transfer.setUpdateCallback((summary)=> {
      progressUpdate({
        tick:        summary.tick,
        transferred: summary.transferred,
        total:       job.count,
        job:         job
      });
    });

    return transfer.transferData(job.index, job.type).then(()=> {
      return manager.completeJob(job);
    }).catch((error)=> {
      const message = `Error: ${JSON.stringify(error)} while processing job: ${JSON.stringify(job)}`;
      progressUpdate({
        message: message,
        level:   'error'
      });
      log.error(message);

      // Requeue entire job on error
      manager.queueJob(job);
      return Promise.resolve();
    }).then(doJob);
  });
};

/**
 * Send update to master if this is a worker, otherwise just print it
 *
 * @param message
 */
const progressUpdate = (message)=> {
  if (overrideProgressUpdate) {
    overrideProgressUpdate(message);
  } else if (!cluster.isMaster) {
    message.pid = process.pid;
    // log.info('message:', message);
    process.send(message);
  } else {
    log.info(`Msg not sent. Not running as worker: ${JSON.stringify(message, null, 2)}`);
  }
};

module.exports = Worker;