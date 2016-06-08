const _              = require('lodash');
const Manager        = require('./manager');
const Transfer       = require('./transfer');
const Progress       = require('../models/progress');
const createEsClient = require('../../config/elasticsearch');
const log            = require('../../config').log;

let RUN_CHECK_INTERVAL_MSEC = 2 * 1000;

const Worker = function (redisClient) {
  const self            = this;
  let updateCallback    = null;
  let completedCallback = null;
  let name              = null;

  self.manager = new Manager(redisClient);

  self.setUpdateCallback = (callback) => {
    updateCallback = callback;
  };

  self.setCompletdCallback = (callback) => {
    completedCallback = callback;
  };

  let killNow = false;
  self.killStopped = () => {
    killNow = true;
  };

  const taskNames = [];

  /**
   * Return the name of the next task to work on.
   * @returns {*}
   */
  const getTaskName = ()=> {
    if (taskNames.length === 0) {
      return self.manager.getTasks().then(tasks => {
        if (tasks.length === 0) {
          return null;
        }

        _.forEach(tasks, task => taskNames.push(task));
        return taskNames.pop();
      });
    } else {
      return taskNames.pop();
    }
  };

  /**
   * Get a task name, then get a subtask within that task to complete.
   *
   * Repeat as long as there are subtasks to complete.
   * @returns {Promise.<TResult>}
   */
  const doSubtask = ()=> {
    return self.manager.isRunning().then(running => {
      if (!running) {
        if (killNow) {
          throw new Error('Worker killed');
        }

        log.info('Currently stopped. Waiting for run...');
        self.manager.workerHeartbeat(name, {status: 'stopped'});  // Not waiting for promise
        return new Promise(resolve => setTimeout(resolve, RUN_CHECK_INTERVAL_MSEC));
      }

      return getTaskName().then(taskName => {
        if (taskName === null) {
          log.info('No tasks found, waiting...');
          self.manager.workerHeartbeat(name, {status: `waiting for task...`});  // Not waiting for promise
          return new Promise(resolve => setTimeout(resolve, RUN_CHECK_INTERVAL_MSEC));
        }

        log.info(`got task: ${taskName}`);
        return self.manager.fetchSubtask(taskName).then((subtask)=> {
          if (!subtask) {
            log.info('No subtask to execute, waiting...');
            self.manager.workerHeartbeat(name, {status: `waiting for subtask...`});  // Not waiting for promise
            return new Promise(resolve => setTimeout(resolve, RUN_CHECK_INTERVAL_MSEC));
          }

          self.manager.workerHeartbeat(name, {
            status:  'starting..',
            task:    taskName,
            subtask: subtask
          });  // Not waiting for promise

          log.info(`got subtask: ${subtask}`);

          return doTransfer(taskName, subtask)
            .then(()=> completeSubtask(taskName, subtask))
            .catch((error)=> {
              const message = `Error: ${JSON.stringify(error)}`;
              self.manager.logError(taskName, subtask, message);

              // Requeue entire subtask on error
              self.manager.queueSubtask(taskName, subtask);
              return Promise.resolve();
            });
        })
      });
    }).then(doSubtask).catch(error => {
      if (error.message === 'Worker killed') {
        log.warn('Worker killed');
      } else {
        throw error;
      }
    });
  };

  const doTransfer = (taskName, subtask) => {
    const source = createEsClient(subtask.source.host, subtask.source.apiVersion);
    const dest   = createEsClient(subtask.destination.host, subtask.destination.apiVersion);

    const transfer = new Transfer(source, dest);

    if (subtask.mutators) {
      transfer.loadMutators(subtask.mutators);
    }

    transfer.setUpdateCallback(update => updateProgress(taskName, subtask, update));

    if (subtask.transfer.documents) {
      return transfer.transferData(subtask.transfer.documents.index, subtask.transfer.documents.type);
    } else if (subtask.transfer.index) {
      return transfer.transferIndices(subtask.transfer.index);
    } else if (subtask.transfer.template) {
      return transfer.transferTemplates(subtask.transfer.template);
    } else {
      log.error(`subtask ${subtask} has unhandled requirements`);
      return;
    }
  };

  /**
   * Update the progress of a specific subtask
   * @param taskName
   * @param subtask
   * @param update
   * @returns {Promise.<TResult>|*}
   */
  const updateProgress = (taskName, subtask, update)=> {
    const progress = new Progress({
      tick:        update.tick,
      transferred: update.transferred,
      total:       subtask.count,
      worker:      name
    });

    self.manager.workerHeartbeat(name, {
      status:   'running',
      task:     taskName,
      subtask:  subtask,
      progress: progress
    });  // Not waiting for promise

    return self.manager.updateProgress(taskName, subtask, progress).then(()=> {
      if (_.isFunction(updateCallback)) {
        updateCallback(taskName, subtask, progress);
      }
    });
  };

  /**
   * Mark a specific subtask as completed
   * @param taskName
   * @param subtask
   * @returns {Promise.<TResult>|*}
   */
  const completeSubtask = (taskName, subtask) => {
    log.info(`completed task: '${taskName}' subtask: ${subtask}`);
    return self.manager.completeSubtask(taskName, subtask).then(()=> {
      if (_.isFunction(completedCallback)) {
        completedCallback(taskName, subtask);
      }
    });
  };

  self.manager.getWorkerName().then(workerName => {
    name = workerName;
    log.info(`Starting worker: ${name}`);
    doSubtask();
  });
};

Worker.__overrideCheckInterval = (intervalMsec)=> {
  RUN_CHECK_INTERVAL_MSEC = intervalMsec;
};


module.exports = Worker;