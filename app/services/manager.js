const _         = require('lodash');
const path      = require('path');
const Promise   = require('bluebird');
const moment    = require('moment');
const sillyname = require('sillyname');

const Task           = require('../models/task');
const Subtask        = require('../models/subtask');
const Progress       = require('../models/progress');
const createEsClient = require('../../config/elasticsearch.js');
const config         = require('../../config/index');
const log            = config.log;


const BACKLOG_QUEUE_KEY = 'backlog_queue';
const BACKLOG_HSET_KEY  = 'backlog_hset';
const COMPLETED_KEY     = 'completed';
const TASK_NAME_KEY     = 'tasks';
const ERROR_KEY         = 'error';
const PROGRESS_KEY      = 'progress';
const RUN_KEY           = 'run';
const WORKER_NAME_KEY   = 'worker_name';
const WORKER_STATUS_KEY = 'worker_status';

let NAME_TIMEOUT_SEC = 10;

let redis = null;

/**
 * Manager constructor
 *
 * The manager prepares and 'manages' the jobs
 * @param sourceEs
 * @param redisClient
 * @constructor
 */
const Manager = function (redisClient) {
  const self = this;

  redis = redisClient;

  self.isRunning  = isRunning;
  self.setRunning = setRunning;

  self.getWorkerName    = reserveWorkerName;
  self.workerHeartbeat  = workerHeartbeat;
  self.getWorkersStatus = getWorkersStatus;

  self.getIndices             = getIndices;
  self.filterDocumentSubtasks = filterDocumentSubtasks;
  self.createFilterFunctions  = createFilterFunctions;

  self.addTask             = addTask;
  self.removeTask          = removeTask;
  self.getTasks            = getTasks;
  self.taskExists          = taskExists;
  self.buildSubtaskBacklog = buildSubtaskBacklog;

  self.fetchSubtask    = fetchSubtask;
  self.queueSubtask    = queueSubtask;
  self.completeSubtask = completeSubtask;

  self.logError  = logError;
  self.getErrors = getErrors;

  self.updateProgress     = updateProgress;
  self.getProgress        = getProgress;
  self.removeProgress     = removeProgress;
  self.getOverallProgress = getOverallProgress;

  self.getCompletedSubtasks = getCompletedSubtasks;
  self.getCompletedCount    = getCompletedCount;
  self.getBacklogSubtasks   = getBacklogSubtasks;
  self.getBacklogCount      = getBacklogCount;

  self.clearBacklogSubtasks   = clearBacklogSubtasks;
  self.clearCompletedSubtasks = clearCompletedSubtasks;

  self._setWorkerName       = setWorkerName;
  self._addCountToSubtasks  = addCountToSubtasks;
  self._overrideNameTimeout = (timeout) => {
    NAME_TIMEOUT_SEC = timeout;
  }
};

/**
 * Pop a job off the queue and return it
 *
 * @returns {Promise.<TResult>}
 */
const fetchSubtask = (taskName)=> {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  return redis.lpop(`${taskName}_${BACKLOG_QUEUE_KEY}`).then((subtaskID)=> {
    if (_.isNull(subtaskID)) {
      return null;
    }

    return redis.hget(`${taskName}_${BACKLOG_HSET_KEY}`, subtaskID).then((count)=> {
      return Subtask.createFromID(subtaskID, count);
    }).then((subtask)=> {
      return redis.hdel(`${taskName}_${BACKLOG_HSET_KEY}`, subtask.getID()).return(subtask);
    });
  });
};

/**
 * Add subtask to queue
 * @param taskName
 * @param subtask
 * @returns {*|Promise.<TResult>}
 */
const queueSubtask = (taskName, subtask)=> {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  if (!(subtask instanceof Subtask)) {
    subtask = new Subtask(subtask);
  }

  return redis.hset(`${taskName}_${BACKLOG_HSET_KEY}`, subtask.getID(), subtask.count).then((numberAdded)=> {
    if (numberAdded === 0) {
      log.warn(`subtask: ${subtask} already in queue`);
      return Promise.resolve();
    } else {
      return redis.rpush(`${taskName}_${BACKLOG_QUEUE_KEY}`, subtask.getID());
    }
  });
};

/**
 * Mark a subtask as completed
 * @param taskName
 * @param subtask
 * @returns {*|{arity, flags, keyStart, keyStop, step}}
 */
const completeSubtask = (taskName, subtask)=> {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  if (!(subtask instanceof Subtask)) {
    subtask = new Subtask(subtask);
  }

  return removeProgress(taskName, subtask).then(()=> {
    return redis.hset(`${taskName}_${COMPLETED_KEY}`, subtask.getID(), subtask.count);
  });
};

/**
 * Clear backlog
 *
 * @returns {Promise.<TResult>}
 */
const clearBacklogSubtasks = (taskName)=> {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  log.info(`clearing existing backlog for task: '${taskName}'`);

  return redis.del(`${taskName}_${BACKLOG_QUEUE_KEY}`).then(()=> {
    return redis.del(`${taskName}_${BACKLOG_HSET_KEY}`);
  });
};

/**
 * Returns all backlog jobs and their counts
 *
 * @returns {Promise.<TResult>}
 */
const getBacklogSubtasks = (taskName)=> {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  return redis.hgetall(`${taskName}_${BACKLOG_HSET_KEY}`).then((jobsAndCounts)=> {
    // ioredis returns an object where the keys are the hash fields and the values are the hash values
    return _.map(jobsAndCounts, (count, subtaskID)=> {
      return Subtask.createFromID(subtaskID, count);
    });
  });
};

/**
 * Get total docs in backlog
 *
 * @returns {Promise.<TResult>}
 */
const getBacklogCount = (taskName)=> {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  return redis.hvals(`${taskName}_${BACKLOG_HSET_KEY}`).then((counts)=> {
    return _.reduce(counts, (total, count)=> {
      total += parseInt(count);
      return total;
    }, 0);
  });
};

/**
 * Returns all completed jobs and their counts
 *
 * @returns {Promise.<TResult>}
 */
const getCompletedSubtasks = (taskName)=> {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  return redis.hgetall(`${taskName}_${COMPLETED_KEY}`).then((jobsAndCounts)=> {
    // ioredis returns an object where the keys are the hash fields and the values are the hash values
    return _.map(jobsAndCounts, (count, subtaskID)=> {
      return Subtask.createFromID(subtaskID, count);
    });
  });
};

/**
 * Get total docs completed
 *
 * @returns {Promise.<TResult>}
 */
const getCompletedCount = (taskName)=> {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  return redis.hvals(`${taskName}_${COMPLETED_KEY}`).then((counts)=> {
    return _.reduce(counts, (total, count)=> {
      total += parseInt(count);
      return total;
    }, 0);
  });
};

/**
 * Clear any completed subtasks
 *
 * @returns {Promise.<TResult>}
 */
const clearCompletedSubtasks = (taskName)=> {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  return redis.del(`${taskName}_${COMPLETED_KEY}`);
};

/**
 * Use subtask definitions to count total docs

 * @param client
 * @param subtasks
 * @returns {*}
 */
const addCountToSubtasks = (client, subtasks)=> {
  log.info(`counting docs for ${subtasks.length} subtasks`);

  return Promise.mapSeries(subtasks, (subtask)=> {
    if (!subtask.transfer.documents) {
      subtask.count = 1;
      return new Subtask(subtask);
    }

    return client.count({
      index: subtask.transfer.documents.index,
      type:  subtask.transfer.documents.type
    }).then((result)=> {
      subtask.count = parseInt(result.count);
      return new Subtask(subtask);
    });
  });
};

/**
 * Convert task into subtasks and queue them for execution
 *
 * @param taskName
 * @param task
 * @returns {*|Promise.<TResult>}
 */
const addTask = (taskName, task)=> {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  if (!(task instanceof Task)) {
    task = new Task(task);
  }

  return taskExists(taskName).then(exists => {
    if (exists) {
      throw new Error(`task: '${taskName}' exists. Delete first.`);
    }

    return redis.sadd(TASK_NAME_KEY, taskName).then(()=> {
      return buildSubtaskBacklog(taskName, task);
    })
  });
};

/**
 * Wipe existing backlog and create new backlog based on provided task and completed subtasks
 * @param taskName
 * @param task
 * @returns {Promise.<TResult>}
 */
const buildSubtaskBacklog = (taskName, task) => {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  if (!(task instanceof Task)) {
    task = new Task(task);
  }

  const taskSource = createEsClient(task.source.host, task.source.apiVersion);

  return clearBacklogSubtasks(taskName).then(() => {
    return Promise.reduce([
      generateIndexSubtasks(taskSource, task),
      generateTemplateSubtasks(taskSource, task),
      generateDocumentSubtasks(taskSource, task)
    ], (allSubtasks, stepSubtasks) => allSubtasks.concat(stepSubtasks), []);
  }).then(potenialSubtasks => {
    log.info(`${potenialSubtasks.length} potential subtasks found`);

    return getCompletedSubtasks(taskName).then((completedSubtasks)=> {
      log.info(`${completedSubtasks.length} completed subtasks exist`);

      const unfinished = _.reduce(potenialSubtasks, (result, potential) => {
        if (!_.find(completedSubtasks, potential)) {
          result.push(potential);
        }

        return result;
      }, []);

      log.info(`${unfinished.length} unfinished subtasks remain`);

      return unfinished;
    });
  }).then(allSubtasks => addCountToSubtasks(taskSource, allSubtasks))
    .then(allSubtasks => {
      return Promise.map(allSubtasks, subtask => queueSubtask(taskName, subtask), {concurrency: 10});
    })
};

const setWorkerName = (getName) => {
  const name = getName();

  return purgeOldWorkerData().then(()=> {
    return redis.zadd(WORKER_NAME_KEY, 'NX', moment().valueOf(), name);
  }).then(result => {
    if (!result) {
      return setWorkerName(getName);
    } else {
      return redis.hset(WORKER_STATUS_KEY, name, 'new').return(name);
    }
  });
};

/**
 * Get the status of all workers
 *
 * @returns {*|{arity, flags, keyStart, keyStop, step}}
 */
const getWorkersStatus = () => {
  return purgeOldWorkerData().then(() => redis.hgetall(WORKER_STATUS_KEY)).then(workersStatus => {
    return _.reduce(workersStatus, (result, status, workerName) => {
      result[workerName] = JSON.parse(status);
      return result;
    }, {});
  });
};

/**
 * Called by the workers to indicate they are alive
 *
 * @param name
 * @param status
 * @returns {Promise.<TResult>}
 */
const workerHeartbeat = (name, status) => {
  return redis.zadd(WORKER_NAME_KEY, moment().valueOf(), name).then(()=> {
    return redis.hset(WORKER_STATUS_KEY, name, JSON.stringify(status));
  }).then(purgeOldWorkerData);
};

/**
 * Based on last heartbeat, removes any dead workers and their statuses.
 *
 * @returns {Promise.<TResult>}
 */
const purgeOldWorkerData = () => {
  return redis.zremrangebyscore(WORKER_NAME_KEY, '-inf', moment().subtract(NAME_TIMEOUT_SEC, 'seconds').valueOf()).then(()=> {
    return redis.zrangebyscore(WORKER_NAME_KEY, '-inf', '+inf');
  }).then(activeWorkerNames => {
    log.debug(`Active workers: ${activeWorkerNames}`);

    return redis.hkeys(WORKER_STATUS_KEY).then(allWorkerNames => {
      log.debug(`All workers: ${allWorkerNames}`);
      return _.difference(allWorkerNames, activeWorkerNames);
    }).then(oldWorkerNames => {
      if (oldWorkerNames.length > 0) {
        log.info(`Expiring status of workers: ${oldWorkerNames}`);
      }
      return Promise.each(oldWorkerNames, oldName => redis.hdel(WORKER_STATUS_KEY, oldName));
    });
  });
};

/**
 * Uses sillyname to reserve a unique name
 */
const reserveWorkerName = () => {
  return setWorkerName(sillyname);
};

const isRunning = () => {
  return redis.get(RUN_KEY).then(running => running === 'running');
};

const setRunning = (running) => {
  const state = running ? 'running' : 'stopped';
  return redis.set(RUN_KEY, state);
};

const getTasks = () => {
  return redis.smembers(TASK_NAME_KEY).then(tasks => tasks || []);
};

const removeProgress = (taskName, subtask) => {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  return redis.hdel(`${taskName}_${PROGRESS_KEY}`, JSON.stringify(subtask));
};

const updateProgress = (taskName, subtask, progress) => {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  progress.lastModified = moment().toISOString();

  if (!(progress instanceof Progress)) {
    progress = new Progress(progress);
  }

  if (!(subtask instanceof Subtask)) {
    subtask = new Subtask(subtask);
  }

  return redis.hset(`${taskName}_${PROGRESS_KEY}`, JSON.stringify(subtask), JSON.stringify(progress));
};

const getProgress = (taskName, subtask) => {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  if (!(subtask instanceof Subtask)) {
    subtask = new Subtask(subtask);
  }

  return redis.hget(`${taskName}_${PROGRESS_KEY}`, JSON.stringify(subtask)).then((progress)=> {
    return JSON.parse(progress);
  })
};

/**
 * Get all known subtask progress updates
 *
 * @param taskName
 * @returns {*|Promise.<TResult>}
 */
const getOverallProgress = (taskName) => {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  return redis.hgetall(`${taskName}_${PROGRESS_KEY}`).then((overallProgress)=> {
    return _.reduce(overallProgress, (result, rawProgress, rawSubtask) => {
      const progress = {
        subtask:  new Subtask(JSON.parse(rawSubtask)),
        progress: new Progress(JSON.parse(rawProgress))
      };

      result.push(progress);
      return result;
    }, []);
  });
};

/**
 * Record an error during a task, with timestamp
 * @param taskName
 * @param subtask
 * @param message
 * @returns {*|{arity, flags, keyStart, keyStop, step}}
 */
const logError = (taskName, subtask, message) => {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  if (!(subtask instanceof Subtask)) {
    subtask = new Subtask(subtask);
  }

  const time_ms = moment().valueOf();
  const error   = {
    subtask: subtask,
    message: message
  };

  log.error(JSON.stringify(error));

  return redis.zadd(`${taskName}_${ERROR_KEY}`, time_ms, JSON.stringify(error));
};

/**
 * Return all errors recorded for a given task
 * @param taskName
 * @returns {*|Promise.<TResult>}
 */
const getErrors = (taskName)=> {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  return redis.zrangebyscore(`${taskName}_${ERROR_KEY}`, '-inf', '+inf', 'WITHSCORES').then((rawErrors)=> {
    const errors = [];

    for (let i = 0; i < rawErrors.length; i += 2) {
      const errorInfo     = JSON.parse(rawErrors[i]);
      errorInfo.subtask   = new Subtask(errorInfo.subtask);
      errorInfo.timestamp = moment(rawErrors[i + 1], 'x').toISOString();

      errors.push(errorInfo);
    }

    return errors;
  });
};

const removeTask = (taskName) => {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  return clearBacklogSubtasks(taskName).then(()=> {
    return clearCompletedSubtasks(taskName);
  }).then(()=> {
    return redis.srem(TASK_NAME_KEY, taskName);
  });
};

const taskExists = (taskName) => {
  if (!_.isString(taskName) || !Task.NAME_REGEX.test(taskName)) {
    throw new Error('taskName must be string of 1-40 alphanumeric characters');
  }

  return redis.sismember(TASK_NAME_KEY, taskName);
};

const generateIndexSubtasks = (client, task) => {
  if (!task.spec.indices || !task.spec.indices.names) {
    log.info('No indices specified in task');
    return Promise.resolve([]);
  }

  return getIndices(client, task.spec.indices.names)
    .then(allIndices => _.map(allIndices, index => index.name));
};

const generateTemplateSubtasks = (client, task) => {
  if (!task.spec.indices || !task.spec.indices.templates) {
    log.info('No templates specified in task');
    return Promise.resolve([]);
  }

  return getTemplates(client, task.spec.indices.templates)
    .then(allTemplates => _.map(allTemplates, template => template.name));
};

/**
 * Generate individual document subtasks from task
 *
 * @param client
 * @param task
 * @returns {*}
 */
const generateDocumentSubtasks = (client, task) => {
  if (!task.spec.documents) {
    log.info('No documents specified in task');
    return Promise.resolve([]);
  }

  return getIndices(client, task.spec.documents.fromIndices).then((allIndices)=> {
    const filters = createFilterFunctions(task.spec.filters);
    return filterDocumentSubtasks(task, allIndices, filters);
  });
};

const filterDocumentSubtasks = (task, allIndices, filters) => {
  let selectedIndices = null;

  if (_.isFunction(filters.indices)) {
    selectedIndices = _.filter(allIndices, filters.indices);
  } else {
    selectedIndices = allIndices;
  }

  return _.reduce(selectedIndices, (result, index)=> {
    let selectedTypes = null;
    const allTypes    = _.reduce(index.mappings, (inner_result, type, name)=> {
      type.name = name;
      inner_result.push(type);

      return inner_result;
    }, []);

    if (_.isFunction(filters.types)) {
      selectedTypes = _.filter(allTypes, filters.types);
    } else {
      selectedTypes = allTypes;
    }

    const typeNames = _.map(selectedTypes, 'name');

    if (typeNames.length > 0) {
      _.forEach(typeNames, typeName => {
        const subtask = {
          source:      task.source,
          destination: task.destination,
          transfer:    {
            documents: {
              index: index.name,
              type:  typeName
            }
          }
        };

        if (task.mutators) {
          subtask.mutators = task.mutators;
        }

        result.push(subtask);
      });
    }

    return result;
  }, []);
};

const createFilterFunctions = (filterSpec) => {
  return _.reduce(filterSpec, (result, filter, name)=> {

    let filterFunction = null;

    if (filter.type === 'path') {
      const extension = path.extname(filter.value);

      if (extension.length > 1) {
        if (extension !== '.js') {
          throw new Error(`filter: '${filter.value}' was interpreted as a path to a non-js file. Must be a path to a module, regex or function`);
        }

        try {
          filterFunction = require(filter.value);
          log.info(`Loaded filter: '${filter.value}' as module`);
        } catch (ex) {
          throw new Error(`filter: '${filter.value}' was interpreted as a path and cannot be found. Must be a path to a module, regex or function`);
        }

        if (!_.isFunction(filterFunction)) {
          throw new Error(`filter: '${filter.value}' was interpreted as a path and module does not return a function. Must be a path to a module, regex or function`);
        }
      }
    } else if (filter.type === 'regex') {
      const regex = new RegExp(filter.value);

      filterFunction = (target)=> {
        log.info('target', target);
        return regex.test(target.name);
      };
      log.info(`Loaded filter: '${filter.value}' as regex`);
    } else {
      throw new Error(`Unexpected filter type '${filter.type}'`);
    }

    result[name] = filterFunction;
    return result;
  }, {});
};

/**
 * Returns an array of the indices found using the elasticsearch multi-index definition.
 *
 * The format is similar to an ES index GET command, but with the name nested in the element.
 *
 * @param client
 * @param targetIndices
 * @returns {Promise.<TResult>}
 */
const getIndices = (client, targetIndices) => {
  return client.indices.get({
    index:          targetIndices,
    allowNoIndices: true
  }).then((response)=> {
    return _.reduce(response, (result, index, name)=> {
      index.name = name;
      result.push(index);

      return result;
    }, []);
  });
};

/**
 * Returns an array of the templates found using the elasticsearch multi-index definition.
 *
 * The format is similar to an ES template GET command, but with the name nested in the element.
 *
 * @param client
 * @param targetTemplates
 * @returns {Promise.<T>}
 */
const getTemplates = (client, targetTemplates) => {
  return client.indices.getTemplate({
    name: targetTemplates
  }).then((templates)=> {
    return _.reduce(templates, (result, template, name)=> {
      template.name = name;
      result.push(template);
      return result;
    }, []);
  }).catch((error)=> {
    if (error.status === 404) {
      log.warn('Templates asked to be copied, but none found');
      return Promise.reject('Templates asked to be copied, but none found');
    }

    return Promise.reject(error);
  });
};

module.exports = Manager;