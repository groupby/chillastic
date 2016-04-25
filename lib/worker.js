import redis from '../config/redis';
import config from '../config';
import createEsClient from '../config/elasticsearch.js'
import Promise from 'bluebird';
import cluster from 'cluster';

var log = config.log;

import Transfer from './transfer';
import Manager from './manager';

var transfer               = null;
var manager                = null;
var overrideProgressUpdate = null;

/**
 * Worker constructor
 *
 * @param sourceUrl
 * @param destUrl
 * @constructor
 */
var Worker = function (sourceUrl, destUrl) {
  var self = this;
  self.source = createEsClient(sourceUrl, '1.4');
  // source      = self.source;
  self.dest   = createEsClient(destUrl, '2.2');
  // dest        = self.dest;

  transfer = new Transfer(self.source, self.dest);
  manager  = new Manager(self.source);

  self._overrideProgresUpdate = (callback)=> {
    overrideProgressUpdate = callback;
  };

  self.start = (exitOnComplete) => {
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
var doJob = ()=> {
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
      var message = 'Error: ' + JSON.stringify(error) + ' while processing job: ' + JSON.stringify(job);
      progressUpdate({
        message: message,
        level:   'error'
      });
      log.error(message);
      return Promise.resolve();
    }).then(doJob);
  });
};

/**
 * Send update to master if this is a worker, otherwise just print it
 *
 * @param message
 */
var progressUpdate = (message)=> {
  if (overrideProgressUpdate) {
    overrideProgressUpdate(message);
  } else if (!cluster.isMaster) {
    message.pid = process.pid;
    process.send(message);
  } else {
    log.info('Msg not sent. Not running as worker: ', JSON.stringify(message, null, 2));
  }
};

export default Worker;