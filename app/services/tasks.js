const _             = require('lodash');
const moment        = require('moment');
const Promise       = require('bluebird');
const Filters       = require('./filters');
const Mutators      = require('./mutators');
const Subtasks      = require('./subtasks');
const ObjectId      = require('../models/objectId');
const Progress      = require('../models/progress');
const Subtask       = require('../models/subtask');
const Task          = require('../models/task');
const config        = require('../../config/index');
const elasticsearch = require('../../config/elasticsearch');

const log = config.log;

const Tasks    = function (redisClient) {
  const self               = this;
  const redis              = redisClient;
  const subtasks           = new Subtasks(redis);
  const mutatorsService    = new Mutators(redis);
  const filtersService     = new Filters(redis);
  const namespacedServices = [
    mutatorsService,
    filtersService
  ];

  /**
   * Return the names of all known tasks
   * @returns {Promise.<*|Array>|*}
   */
  self.getAll = () => redis.smembers(Tasks.NAME_KEY).then((tasks) => tasks || []);

  /**
   * Check that source and destination configurations work
   * @param source
   * @param dest
   * @returns {*}
   */
  self.ensureSourceAndDestExist = (source, dest) => {
    if (!_.isObject(source)) {
      return Promise.reject('source elasticsearch config must be an object');
    }

    if (!_.isObject(dest)) {
      return Promise.reject('dest elasticsearch config must be an object');
    }

    try {
      elasticsearch(source);
    } catch (error) {
      log.error(error);
      return Promise.reject(`Could not connect to source elasticsearch with configuration: ${JSON.stringify(source)}`);
    }

    try {
      elasticsearch(dest);
    } catch (error) {
      log.error(error);
      return Promise.reject(`Could not connect to destination elasticsearch with configuration: ${JSON.stringify(dest)}`);
    }

    return Promise.resolve();
  };

  /**
   * Convert task into subtasks and queue them for execution
   *
   * @param taskId
   * @param task
   * @returns {*|Promise.<TResult>}
   */
  self.add = (taskId, task) => self.exists(taskId)
    .then((exists) => {
      if (exists) {
        throw new Error(`task: '${taskId}' exists. Delete first.`);
      }
    })
    .then(() => self.ensureSourceAndDestExist(task.source, task.destination))
    .then(() => mutatorsService.ensureMutatorsExist(taskId, task.mutators))
    .then(() => filtersService.ensureFiltersExist(taskId, task.transfer.documents.filters))
    .then(() => redis.sadd(Tasks.NAME_KEY, taskId))
    .then(() => subtasks.buildBacklog(taskId, Task.coerce(task)));

  /**
   * Remove a task by name
   * @param taskId
   * @returns {Promise.<TResult>}
   */
  self.remove = (taskId) => Task.validateId(taskId)
    .then(() => subtasks.clearBacklog(taskId))
    .then(() => subtasks.clearCompleted(taskId))
    .then(() => subtasks.clearTotal(taskId))
    .then(() => _.map(namespacedServices, (service) => service.removeAllNamespacedBy(new ObjectId({
      namespace: taskId,
      id:        'dummy'
    }))))
    .then(() => redis.srem(Tasks.NAME_KEY, taskId));

  /**
   * Return TRUE if a task exists in the system based on it's name
   * @param taskId
   * @returns {*|{arity, flags, keyStart, keyStop, step}}
   */
  self.exists = (taskId) => Task.validateId(taskId)
    .then(() => redis.sismember(Tasks.NAME_KEY, taskId));

  /**
   * Record an error during a task, with timestamp
   * @param taskId
   * @param subtask
   * @param message
   * @returns {*|{arity, flags, keyStart, keyStop, step}}
   */
  self.logError = (taskId, subtask, message) => {
    const time_ms = moment().valueOf();
    const error   = JSON.stringify({
      message,
      subtask: Subtask.coerce(subtask)
    });
    log.error(error);

    return Task.validateId(taskId)
      .then(() => redis.zadd(Task.errorKey(taskId), time_ms, error));
  };

  /**
   * Return all errors recorded for a given task
   * @param taskId
   * @returns {*|Promise.<TResult>}
   */
  self.errors = (taskId) => Task.validateId(taskId)
    .then(() => redis.zrangebyscore(Task.errorKey(taskId), '-inf', '+inf', 'WITHSCORES'))
    .then((rawErrors) => {
      const skip   = 2;
      const errors = [];
      for (let i = 0; i < rawErrors.length; i += skip) {
        const error = JSON.parse(rawErrors[i]);
        errors.push(_.assign(error, {
          subtask:   new Subtask(error.subtask),
          timestamp: moment(rawErrors[i + 1], 'x').toISOString()
        }));
      }
      return errors;
    });

  /**
   * Get all known subtask progress updates
   *
   * @param taskId
   * @returns {*|Promise.<TResult>}
   */
  self.getProgress = (taskId) => Task.validateId(taskId)
    .then(() => redis.hgetall(Task.progressKey(taskId)))
    .then((overallProgress) => _.reduce(overallProgress, (result, rawProgress, rawSubtask) => {
      const progress = Progress.coerce(JSON.parse(rawProgress));
      const subtask  = Subtask.coerce(_.assign(JSON.parse(rawSubtask), {count: progress.total}));
      return result.concat({subtask, progress});
    }, []));
};
Tasks.NAME_KEY = 'tasks';

module.exports = Tasks;