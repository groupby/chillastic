const _        = require('lodash');
const moment   = require('moment');
const Filters  = require('./filters');
const Mutators = require('./mutators');
const Subtasks = require('./subtasks');
const ObjectId = require('../models/objectId');
const Progress = require('../models/progress');
const Subtask  = require('../models/subtask');
const Task     = require('../models/task');
const config   = require('../../config/index');

const log = config.log;

const Tasks    = function (redisClient) {
  const self               = this;
  const redis              = redisClient;
  const subtasks           = new Subtasks(redis);
  const namespacedServices = [
    new Mutators(redis),
    new Filters(redis)
  ];

  /**
   * Return the names of all known tasks
   * @returns {Promise.<*|Array>|*}
   */
  self.getAll = () => redis.smembers(Tasks.NAME_KEY).then((tasks)=> tasks || []);

  /**
   * Convert task into subtasks and queue them for execution
   *
   * @param taskId
   * @param task
   * @returns {*|Promise.<TResult>}
   */
  self.add = (taskId, task)=>
      self.exists(taskId)
      .then(exists => {
        if (exists) {
          throw new Error(`task: '${taskId}' exists. Delete first.`);
        }
      })
      .then(()=> redis.sadd(Tasks.NAME_KEY, taskId))
      .then(()=> subtasks.buildBacklog(taskId, Task.coerce(task)));

  /**
   * Remove a task by name
   * @param taskId
   * @returns {Promise.<TResult>}
   */
  self.remove = (taskId) =>
      Task.validateId(taskId)
      .then(()=> subtasks.clearBacklog(taskId))
      .then(()=> subtasks.clearCompleted(taskId))
      .then(()=> _.map(namespacedServices, (service)=> service.removeAllNamespacedBy(new ObjectId({namespace: taskId, id: 'dummy'}))))
      .then(()=> redis.srem(Tasks.NAME_KEY, taskId));

  /**
   * Return TRUE if a task exists in the system based on it's name
   * @param taskId
   * @returns {*|{arity, flags, keyStart, keyStop, step}}
   */
  self.exists = (taskId) =>
      Task.validateId(taskId)
      .then(()=> redis.sismember(Tasks.NAME_KEY, taskId));

  /**
   * Record an error during a task, with timestamp
   * @param taskId
   * @param subtask
   * @param message
   * @returns {*|{arity, flags, keyStart, keyStop, step}}
   */
  self.logError = (taskId, subtask, message) => {
    const time_ms = moment().valueOf();
    const error   = JSON.stringify({message, subtask: Subtask.coerce(subtask)});
    log.error(error);

    return Task.validateId(taskId)
    .then(()=> redis.zadd(Task.errorKey(taskId), time_ms, error));
  };

  /**
   * Return all errors recorded for a given task
   * @param taskId
   * @returns {*|Promise.<TResult>}
   */
  self.errors = (taskId)=>
      Task.validateId(taskId)
      .then(() => redis.zrangebyscore(Task.errorKey(taskId), '-inf', '+inf', 'WITHSCORES'))
      .then((rawErrors)=> {
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
  self.getProgress = (taskId) =>
      Task.validateId(taskId)
      .then(()=> redis.hgetall(Task.progressKey(taskId)))
      .then((overallProgress)=>
          _.reduce(overallProgress, (result, rawProgress, rawSubtask) => result.concat({
            subtask:  new Subtask(JSON.parse(rawSubtask)),
            progress: new Progress(JSON.parse(rawProgress))
          }), [])
      );
};
Tasks.NAME_KEY = 'tasks';

module.exports = Tasks;