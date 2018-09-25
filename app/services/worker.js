const _              = require('lodash');
const Promise        = require('bluebird');
const Manager        = require('./manager');
const Mutators       = require('./mutators');
const Subtasks       = require('./subtasks');
const Tasks          = require('./tasks');
const Transfer       = require('./transfer');
const Progress       = require('../models/progress');
const createEsClient = require('../../config/elasticsearch');
const log            = require('../../config').log;

let RUN_CHECK_INTERVAL_MS = 2000;

const Worker = function (redisClient) {
  const self            = this;
  let updateCallback    = null;
  let completedCallback = null;
  let name              = null;

  const manager  = new Manager(redisClient);
  const mutators = new Mutators(redisClient);
  const subtasks = new Subtasks(redisClient);
  const tasks    = new Tasks(redisClient);

  self.setUpdateCallback = (callback) => {
    updateCallback = callback;
  };

  self.setCompletedCallback = (callback) => {
    completedCallback = callback;
  };

  let killNow      = false;
  self.killStopped = () => {
    killNow = true;
  };

  /**
   * Return the name of the next task to work on.
   * @returns {*}
   */
  const taskIds     = [];
  const getTaskName = () => taskIds.length !== 0 ? Promise.resolve(taskIds.pop()) : tasks.getAll()
    .then((allTasks) => {
      if (allTasks.length === 0) {
        return null;
      } else {
        allTasks.forEach((task) => taskIds.push(task));
        return taskIds.pop();
      }
    });

  const timeoutPromise = (timeout) => new Promise((resolve) => setTimeout(resolve, timeout));

  /**
   * Get a task name, then get a subtask within that task to complete.
   *
   * Repeat as long as there are subtasks to complete.
   * @returns {Promise.<TResult>}
   */
  const doSubtask = () => manager.isRunning()
    .then((running) => {
      if (!running) {
        if (killNow) {
          throw new Error('Worker killed');
        }

        log.info('Currently stopped. Waiting for run...');
        manager.workerHeartbeat(name, {status: 'stopped'}); // Not waiting for promise
        return timeoutPromise(RUN_CHECK_INTERVAL_MS);
      }

      return getTaskName()
        .then((taskId) => {
          if (taskId === null) {
            log.trace('No tasks found, waiting...');
            manager.workerHeartbeat(name, {status: 'waiting for task...'}); // Not waiting for promise
            return timeoutPromise(RUN_CHECK_INTERVAL_MS);
          }

          return subtasks.countBacklog(taskId).then((backlogCount) => {
            if (backlogCount === 0) {
              log.trace('No tasks found, waiting...');
              manager.workerHeartbeat(name, {status: 'waiting for task...'}); // Not waiting for promise
              return timeoutPromise(RUN_CHECK_INTERVAL_MS);
            }

            log.info(`got task: ${taskId}`);

            return subtasks.fetch(taskId)
              .then((subtask) => {
                if (!subtask) {
                  log.trace('No subtask to execute, waiting...');
                  manager.workerHeartbeat(name, {status: 'waiting for subtask...'}); // Not waiting for promise
                  return timeoutPromise(RUN_CHECK_INTERVAL_MS);
                }

                manager.workerHeartbeat(name, {
                  status: 'starting..',
                  task:   taskId,
                  subtask
                }); // Not waiting for promise

                log.info(`got subtask: ${subtask}`);

                return doTransfer(taskId, subtask)
                  .then(() => completeSubtask(taskId, subtask))
                  .catch((error) => {
                    tasks.logError(taskId, subtask, `Error: ${JSON.stringify(error)}`);
                    return Promise.resolve();
                  });
              });
          });
        });
    })
    .then(doSubtask)
    .catch((error) => {
      if (error.message === 'Worker killed') {
        log.warn('Worker killed');
      } else {
        throw error;
      }
    });

  const doTransfer = (taskId, subtask) => {
    const source = createEsClient(subtask.source);
    const dest   = createEsClient(subtask.destination);

    const transfer = new Transfer(source, dest);
    if (subtask.mutators) {
      mutators.load(taskId, subtask.mutators).then(transfer.setMutators);
    }
    transfer.setUpdateCallback((update) => updateProgress(taskId, subtask, update));

    if (subtask.transfer.documents) {
      return transfer.transferData(subtask.transfer.documents.index, subtask.transfer.documents.type, subtask.transfer.flushSize, subtask.transfer.documents.minSize, subtask.transfer.documents.maxSize);
    } else if (subtask.transfer.index) {
      return transfer.transferIndices(subtask.transfer.index);
    } else if (subtask.transfer.template) {
      return transfer.transferTemplates(subtask.transfer.template);
    } else {
      log.error(`subtask ${subtask} has unhandled requirements`);
    }
  };

  /**
   * Update the progress of a specific subtask
   * @param taskId
   * @param subtask
   * @param update
   * @returns {Promise.<TResult>|*}
   */
  const updateProgress = (taskId, subtask, update) => {
    const progress = new Progress({
      tick:        update.tick,
      transferred: update.transferred,
      total:       subtask.count,
      worker:      name
    });

    manager.workerHeartbeat(name, {
      status:   'running',
      task:     taskId,
      subtask,
      progress: progress
    }); // Not waiting for promise

    return subtasks.updateProgress(taskId, subtask, progress)
      .then(() => {
        if (_.isFunction(updateCallback)) {
          updateCallback(taskId, subtask, progress);
        }
      });
  };

  /**
   * Mark a specific subtask as completed
   * @param taskId
   * @param subtask
   * @returns {Promise.<TResult>|*}
   */
  const completeSubtask = (taskId, subtask) => {
    log.info(`completed task: '${taskId}' subtask: ${subtask}`);
    return subtasks.complete(taskId, subtask)
      .then(() => {
        if (_.isFunction(completedCallback)) {
          return completedCallback(taskId, subtask);
        }
      });
  };

  manager.getWorkerName().then((workerName) => {
    name = workerName;
    log.info(`Starting worker: ${name}`);
    doSubtask();
  });
};

Worker.__overrideCheckInterval = (intervalMsec) => {
  RUN_CHECK_INTERVAL_MS = intervalMsec;
};

module.exports = Worker;