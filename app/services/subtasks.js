const _              = require('lodash');
const moment         = require('moment');
const Promise        = require('bluebird');
const Filters        = require('./filters');
const Transfer       = require('./transfer');
const Progress       = require('../models/progress');
const Subtask        = require('../models/subtask');
const Task           = require('../models/task');
const createEsClient = require('../../config/elasticsearch');
const config         = require('../../config/index');

const log = config.log;

const Subtasks = function (redisClient) {
  const self    = this;
  const redis   = redisClient;
  const filters = new Filters(redis);

  /**
   * Pop a job off the queue and return it
   *
   * @returns {Promise.<TResult>}
   */
  self.fetch = (taskId)=> {
    return Task.validateId(taskId)
    .then(()=> redis.lpop(Task.backlogQueueKey(taskId)))
    .then((subtaskID)=> {
      log.info('ID:', subtaskID);
      return subtaskID;
    })
    .then((subtaskID)=>
        _.isNull(subtaskID) ? null : redis.hget(Task.backlogHSetKey(taskId), subtaskID)
        .then((count)=> Subtask.createFromID(subtaskID, count))
        .then((subtask)=> redis.hdel(Task.backlogHSetKey(taskId), subtask.getID()).return(subtask))
    );
  };

  /**
   * Add subtask to queue
   * @param taskId
   * @param subtask
   * @returns {*|Promise.<TResult>}
   */
  self.queue = (taskId, subtask)=> {
    subtask = Subtask.coerce(subtask);

    return Task.validateId(taskId)
    .then(()=> redis.hset(Task.backlogHSetKey(taskId), subtask.getID(), subtask.count))
    .then((numberAdded)=> {
      if (numberAdded === 0) {
        log.warn(`subtask: ${subtask} already in queue`);
        return Promise.resolve();
      } else {
        return redis.rpush(Task.backlogQueueKey(taskId), subtask.getID());
      }
    });
  };

  /**
   * Mark a subtask as completed
   * @param taskId
   * @param subtask
   * @returns {*|{arity, flags, keyStart, keyStop, step}}
   */
  self.complete = (taskId, subtask)=> {
    subtask = Subtask.coerce(subtask);

    return Task.validateId(taskId)
    .then(()=> self.removeProgress(taskId, subtask))
    .then(()=> redis.hset(Task.completedKey(taskId), subtask.getID(), subtask.count));
  };

  const incrementCount = (subtask, increment)=> {
    subtask.count = parseInt(increment);
    return new Subtask(subtask);
  };

  /**
   * Use subtask definition to get total doc count from ES

   * @param client
   * @param subtask
   * @returns {*}
   */
  self.addCount = (client, subtask)=>
      subtask.transfer.documents ?
          client.count({
            index: subtask.transfer.documents.index,
            type:  subtask.transfer.documents.type
          })
          .then((result)=> incrementCount(subtask, result.count)) :
          incrementCount(subtask, 1);

  /**
   * Given a task, create a list of index configuration transfer subtasks
   *
   * @param client
   * @param task
   * @returns {Promise.<Array>}
   */
  const generateIndexSubtasks = (client, task) => {
    if (!task.transfer.indices || !task.transfer.indices.names) {
      log.info('No index subtasks specified in task');
      return Promise.resolve([]);
    } else {
      return Transfer.getIndices(client, task.transfer.indices.names)
      .then(allIndices => allIndices.map((index)=> index.name));
    }
  };

  /**
   * Given a task, create a list of template transfer subtasks
   *
   * @param client
   * @param task
   * @returns {Promise.<Array>}
   */
  const generateTemplateSubtasks = (client, task) => {
    if (!task.transfer.indices || !task.transfer.indices.templates) {
      log.info('No template subtasks specified in task');
      return Promise.resolve([]);
    } else {
      return Transfer.getTemplates(client, task.transfer.indices.templates)
      .then(allTemplates => allTemplates.map((template)=> template.name));
    }
  };

  /**
   * Generate individual document subtasks from task
   *
   * @param client
   * @param taskId
   * @param task
   * @returns {*}
   */
  const generateDocumentSubtasks = (client, taskId, task) => {
    if (!task.transfer.documents) {
      log.info('No documents specified in task');
      return Promise.resolve([]);
    } else {
      const loadedFilters = task.transfer.documents.filters ? filters.load(taskId, task.transfer.documents.filters) : {};
      return Transfer.getIndices(client, task.transfer.documents.fromIndices)
      .then((allIndices)=> self.filterDocumentSubtasks(task, allIndices, loadedFilters));
    }
  };

  /**
   * Given a task, all relevant indices, and filters, return the list of subtasks.
   * @param task
   * @param allIndices
   * @param loadedFilters
   * @returns {*}
   */
  self.filterDocumentSubtasks = (task, allIndices, loadedFilters) => {
    const predicate                = (allFilters)=> (input)=> allFilters.reduce((result, filter)=> result || filter.predicate(input), false);
    const getTypesFromMappings     = (mappings)=> _.reduce(mappings, (result, type, name)=> result.concat(_.assign(type, {name})), []);
    const generatePotentialSubtask = (indexName, typeName)=> _.omitBy({
      source:      task.source,
      destination: task.destination,
      transfer:    {
        documents: {
          index: indexName,
          type:  typeName
        }
      },
      mutators:    task.mutators
    }, _.isUndefined);

    const filteredIndices = _.isArray(loadedFilters.index) ? allIndices.filter(predicate(loadedFilters.index)) : allIndices;
    return filteredIndices.reduce((result, index)=> {
      const allTypes      = getTypesFromMappings(index.mappings);
      const filteredTypes = _.isArray(loadedFilters.type) ? allTypes.filter(predicate(loadedFilters.type)) : allTypes;

      filteredTypes.forEach((filteredType)=> result.push(generatePotentialSubtask(index.name, filteredType.name)));
      return result;
    }, []);
  };

  /**
   * Wipe existing backlog and create new backlog based on provided task and completed subtasks
   * @param taskId
   * @param task
   * @returns {Promise.<TResult>}
   */
  self.buildBacklog = (taskId, task) => {
    task             = Task.coerce(task);
    const taskSource = createEsClient(task.source);

    return Task.validateId(taskId)
    .then(()=> self.clearBacklog(taskId))
    .then(() =>
        Promise.reduce([
          generateIndexSubtasks(taskSource, task),
          generateTemplateSubtasks(taskSource, task),
          generateDocumentSubtasks(taskSource, taskId, task)
        ], (allSubtasks, stepSubtasks) => allSubtasks.concat(stepSubtasks), [])
    )
    .then(potentialSubtasks => {
      log.info(`${potentialSubtasks.length} potential subtasks found`);

      return self.getCompleted(taskId)
      .then((completedSubtasks)=> {
        log.info(`${completedSubtasks.length} completed subtasks exist`);
        const unfinished = potentialSubtasks.filter((potential) => !_.find(completedSubtasks, potential));

        log.info(`${unfinished.length} unfinished subtasks remain`);
        return unfinished;
      });
    })
    .then(allSubtasks => Promise.map(allSubtasks, (subtask)=> self.addCount(taskSource, subtask), {concurrency: 10}))
    .then(allSubtasks => Promise.map(allSubtasks, (subtask)=> self.queue(taskId, subtask), {concurrency: 10}));
  };

  /**
   * Clear backlog
   *
   * @returns {Promise.<TResult>}
   */
  self.clearBacklog = (taskId)=>
      Task.validateId(taskId)
      .then(()=> log.info(`clearing existing backlog for task: '${taskId}'`))
      .then(()=> redis.del(Task.backlogQueueKey(taskId)))
      .then(()=> redis.del(Task.backlogHSetKey(taskId)));

  /**
   * Returns all backlog jobs and their counts
   *
   * @returns {Promise.<TResult>}
   */
  self.getBacklog = (taskId)=>
      Task.validateId(taskId)
      .then(()=> redis.hgetall(Task.backlogHSetKey(taskId)))
      .then((jobsAndCounts)=>
          // ioredis returns an object where the keys are the hash fields and the values are the hash values
          _.map(jobsAndCounts, (count, subtaskID)=> Subtask.createFromID(subtaskID, count)));

  /**
   * Get total docs in backlog
   *
   * @returns {Promise.<TResult>}
   */
  self.countBacklog = (taskId)=>
      Task.validateId(taskId)
      .then(()=> redis.hvals(Task.backlogHSetKey(taskId)))
      .then((counts)=> counts.reduce((total, count)=> total + parseInt(count), 0));

  /**
   * Clear any completed subtasks
   *
   * @returns {Promise.<TResult>}
   */
  self.clearCompleted = (taskId)=>
      Task.validateId(taskId)
      .then(()=> redis.del(Task.completedKey(taskId)));

  /**
   * Returns all completed jobs and their counts
   *
   * @returns {Promise.<TResult>}
   */
  self.getCompleted = (taskId)=>
      Task.validateId(taskId)
      .then(()=> redis.hgetall(Task.completedKey(taskId)))
      .then((jobsAndCounts)=>
          // ioredis returns an object where the keys are the hash fields and the values are the hash values
          _.map(jobsAndCounts, (count, subtaskID)=> Subtask.createFromID(subtaskID, count))
      );

  /**
   * Get total docs completed
   *
   * @returns {Promise.<TResult>}
   */
  self.countCompleted = (taskId)=>
      Task.validateId(taskId)
      .then(()=> redis.hvals(Task.completedKey(taskId)))
      .then((counts)=> counts.reduce((total, count)=> total + parseInt(count), 0));

  /**
   * Clear the progress of a given subtask within a task
   * @param taskId
   * @param subtask
   * @returns {*|{arity, flags, keyStart, keyStop, step}}
   */
  self.removeProgress = (taskId, subtask) =>
      Task.validateId(taskId)
      .then(() => redis.hdel(Task.progressKey(taskId), JSON.stringify(subtask)));

  /**
   * Update the progress of a given subtask within a task
   * @param taskId
   * @param subtask
   * @param progress
   * @returns {*|{arity, flags, keyStart, keyStop, step}}
   */
  self.updateProgress = (taskId, subtask, progress) => {
    progress              = Progress.coerce(progress);
    progress.lastModified = moment().toISOString();

    return Task.validateId(taskId)
    .then(()=> redis.hset(Task.progressKey(taskId), JSON.stringify(Subtask.coerce(subtask)), JSON.stringify(progress)));
  };

  /**
   * Get the progress of a given subtask within a task
   * @param taskId
   * @param subtask
   * @returns {Promise.<TResult>|*}
   */
  self.getProgress = (taskId, subtask) =>
      Task.validateId(taskId)
      .then(()=> redis.hget(Task.progressKey(taskId), JSON.stringify(Subtask.coerce(subtask))))
      .then((progress)=> JSON.parse(progress));
};

module.exports = Subtasks;