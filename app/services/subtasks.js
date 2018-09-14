const _              = require('lodash');
const moment         = require('moment');
const Promise        = require('bluebird');
const prettyBytes    = require('prettysize');
const Filters        = require('./filters');
const Transfer       = require('./transfer');
const Progress       = require('../models/progress');
const Subtask        = require('../models/subtask');
const Task           = require('../models/task');
const createEsClient = require('../../config/elasticsearch');
const config         = require('../../config/index');
const to_bytes       = require('../../config/utils').to_bytes;

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
  self.fetch = (taskId) => {
    return Task.validateId(taskId)
    .then(() => redis.lpop(Task.backlogQueueKey(taskId)))
    .then((subtaskID) => {
      log.info('ID:', subtaskID);
      return subtaskID;
    })
    .then((subtaskID) => _.isNull(subtaskID) ? null : redis.hget(Task.backlogHSetKey(taskId), subtaskID)
    .then((count) => Subtask.createFromID(subtaskID, count))
    .then((subtask) => redis.hdel(Task.backlogHSetKey(taskId), subtask.getID()).return(subtask)));
  };

  /**
   * Add subtask to queue
   * @param taskId
   * @param subtasks
   * @returns {*|Promise.<TResult>}
   */
  self.queue = (taskId, subtasks) => {
    subtasks = _.map(subtasks, (s) => Subtask.coerce(s));

    return Task.validateId(taskId)
    .then(() => {
      const transaction = redis.multi();
      subtasks.forEach((subtask) => transaction.hset(Task.backlogHSetKey(taskId), subtask.getID(), subtask.count));
      return transaction.exec();
    })
    .then((results) => {
      const transaction = redis.multi();
      for (let i = 0; i < subtasks.length; i += 1) {
        const subtask = subtasks[i];
        if (results[i][1] === 0) {
          log.warn(`subtask: ${subtask} already in queue`);
        } else {
          transaction.rpush(Task.backlogQueueKey(taskId), subtask.getID());
        }
      }
      return transaction.exec();
    });
  };

  /**
   * Mark a subtask as completed
   * @param taskId
   * @param subtask
   * @returns {*|{arity, flags, keyStart, keyStop, step}}
   */
  self.complete = (taskId, subtask) => {
    subtask = Subtask.coerce(subtask);
    return Task.validateId(taskId)
    .then(() => self.removeProgress(taskId, subtask))
    .then(() => redis.hset(Task.completedKey(taskId), subtask.getID(), subtask.count));
  };

  const incrementCount = (subtask, increment) => {
    subtask.count = parseInt(increment);
    return new Subtask(subtask);
  };

  /**
   * Use subtask definition to get total doc count from ES

   * @param client
   * @param subtask
   * @returns {*}
   */
  self.addCount = (client, subtask) => subtask.transfer.documents ? client.count({
    index: subtask.transfer.documents.index,
    type:  subtask.transfer.documents.type
  })
  .then((result) => incrementCount(subtask, result.count)) : incrementCount(subtask, 1);

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
      .then((allIndices) => allIndices.map((index) => index.name));
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
      .then((allTemplates) => allTemplates.map((template) => template.name));
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
      return filters.load(taskId, task.transfer.documents.filters)
      .then((loadedFilters) => Transfer.getIndices(client, task.transfer.documents.fromIndices)
      .then((allIndices) => self.filterDocumentSubtasks(task, allIndices, loadedFilters)));
    }
  };

  /**
   * Given a task, all relevant indices, and filters, return the list of subtasks.
   * @param task
   * @param allIndices
   * @param loadedFilters
   * @returns {*}
   */
  self.filterDocumentSubtasks = (task, allIndices, loadedFilters, boundsField) => {
    const esBoundsField        = boundsField || '_size';
    const predicate            = (allFilters) => (input) => allFilters.reduce((result, filter) => result || filter.predicate(input), false);
    const getTypesFromMappings = (mappings) => _.reduce(mappings, (result, type, name) => result.concat(_.assign(type, {name})), []);
    const newSubtask           = (indexName, typeName, minSize, maxSize, flushSize) => _.omitBy({
      source:      task.source,
      destination: task.destination,
      transfer:    {
        flushSize: flushSize || Subtask.DEFAULT_FLUSH_SIZE,
        documents: {
          index: indexName,
          type:  typeName,
          minSize,
          maxSize,
        }
      },
      mutators: task.mutators
    }, _.isUndefined);

    const esClient        = createEsClient(task.source);
    const filtered        = (items, predicates) => items.filter(_.isArray(predicates) ? predicate(predicates) : () => true);
    const newTypeInfo     = (index, type) => {
      return {index: index.name, type: type.name};
    };
    const newBound        = (minSize, maxSize, flushSize) => {
      return {minSize, maxSize, flushSize: flushSize || Subtask.DEFAULT_FLUSH_SIZE};
    };
    const makeBounds      = (bucket1, bucket2, bucket3) =>
        [bucket1, bucket2, bucket3].filter((b) => b.count > 0).map((b) => newBound(b.minSize, b.maxSize, b.flushSize));
    const closeEnough     = (lhs, rhs) => Math.abs(lhs.chunks - rhs.chunks) < 100;
    const increase        = (value, multiplier) => Math.ceil(value * multiplier);
    const decrease        = (value, multiplier) => Math.floor(value / multiplier);
    const multiplier      = (lhs, rhs) => {
      const delta = Math.abs(lhs.chunks - rhs.chunks);
      if (delta < 200) {
        return 1.1;
      } else if (delta < 500) {
        return 2;
      } else if (delta < 1000) {
        return 3;
      } else if (delta < 4000) {
        return 5;
      } else if (delta < 10000) {
        return 8;
      } else {
        return 10;
      }
    };
    const initialBounds   = (index, type) =>
        esClient.search({
          index, type,
          ignoreUnavailable: true,
          allowNoIndices:    true,
          body:              {size: 0, aggregations: {stats: {stats: {field: esBoundsField}}}}
        })
        .then((response) => {
          const count      = _.get(response, 'aggregations.stats.count', 0);
          const lowerBound = _.get(response, 'aggregations.stats.min', 0);
          const upperBound = _.get(response, 'aggregations.stats.max', 0) + 1;
          if (count === 0) {
            return [0, 0, 0];
          } else if (lowerBound + 1 === upperBound) {
            return [upperBound, upperBound, upperBound];
          } else {
            const piece   = Math.max(1, Math.floor((upperBound - lowerBound) / 10));
            let boundary1 = (6 * piece) + lowerBound;
            let boundary2 = (9 * piece) + lowerBound;
            if (upperBound > to_bytes(1, 'MB')) {
              boundary2 = to_bytes(1, 'MB');
            }
            if (boundary1 > boundary2) {
              boundary1 = decrease(boundary2, 2);
            }
            return [boundary1, boundary2, upperBound];
          }
        });
    const calculateBounds = (i, maxIterations, index, type, bounds) =>
        esClient.search({
          index, type,
          ignoreUnavailable: true,
          allowNoIndices:    true,
          body:              {
            size:         0,
            aggregations: {
              sizes: {
                range: {
                  field:  esBoundsField,
                  ranges: [
                    {key: 'bucket1', from: 0, to: bounds[0]},
                    {key: 'bucket2', from: bounds[0], to: bounds[1]},
                    {key: 'bucket3', from: bounds[1], to: bounds[2]},
                  ]
                }
              }
            }
          }
        })
        .then((response) => {
          const buckets = _.get(response, 'aggregations.sizes.buckets', []);
          const shards  = _.get(response, '_shards.total', 1);
          if (response.hits.total > buckets.reduce((result, count) => result + count.doc_count, 0)) {
            return null;
          } else {
            return _.reduce(buckets, (result, bucket) => {
              const count        = bucket.doc_count;
              const flushSize    = Math.max(1, decrease(to_bytes(50, 'MB'), (bucket.to - 1) * shards));
              result[bucket.key] = {
                count, flushSize,
                chunks:  Math.ceil(count / flushSize),
                minSize: bucket.from,
                maxSize: bucket.to,
              };
              return result;
            }, {});
          }
        })
        .then((buckets) => {
          const prettyLog = (value, padding) => String(value).padStart(padding || 10);
          _.reduce(buckets, (result, value) => {
            try {
              log.info(`count${prettyLog(value.count)} | flush${prettyLog(value.flushSize)} | chunks${prettyLog(value.chunks)} | minSize${prettyLog(prettyBytes(value.minSize))} | maxSize${prettyLog(prettyBytes(value.maxSize))}`);
            } catch (e) {
              log.error(`Unable to log: ${e}`);
            }
          }, null);
          if (buckets) {
            const bucket1 = buckets.bucket1;
            const bucket2 = buckets.bucket2;
            const bucket3 = buckets.bucket3;
            if (bucket1.maxSize === bucket2.maxSize && bucket2.maxSize === bucket3.maxSize) {
              return [newBound(-1, -1, bucket1.maxSize === 0 ? Subtask.DEFAULT_FLUSH_SIZE : bucket1.flushSize)];
            } else if (i >= maxIterations) {
              return makeBounds(bucket1, bucket2, bucket3);
            } else {
              const minBound2 = Math.min(to_bytes(1, 'MB'), bucket3.maxSize / 2);
              let bound1      = bucket1.maxSize;
              let bound2      = bucket2.maxSize;
              if (closeEnough(bucket1, bucket2) && closeEnough(bucket2, bucket3)) {
                return makeBounds(bucket1, bucket2, bucket3);
              } else if (closeEnough(bucket1, bucket2)) {
                const m = multiplier(bucket2, bucket3);
                const f = bucket2.chunks > bucket3.chunks ? decrease : increase;
                bound1 = f(bucket1.maxSize, m);
                bound2 = f(bucket2.maxSize, m);
              } else if (closeEnough(bucket2, bucket3)) {
                if (bound2 === minBound2) {
                  return makeBounds(bucket1, bucket2, bucket3);
                } else {
                  const m = multiplier(bucket1, bucket2);
                  const f = bucket1.chunks > bucket2.chunks ? decrease : increase;
                  bound1 = f(bucket1.maxSize, m);
                  bound2 = f(bucket2.maxSize, m);
                }
              } else {
                const m1 = multiplier(bucket1, bucket2);
                const m2 = multiplier(bucket2, bucket3);
                if (bound2 === minBound2) {
                  if (bucket1.chunks < bucket2.chunks * 10) {
                    bound1 = increase(bucket1.maxSize, m1);
                  } else {
                    return makeBounds(bucket1, bucket2, bucket3);
                  }
                } else if (bucket1.chunks < bucket2.chunks && bucket2.chunks < bucket3.chunks) {
                  bound1 = increase(bucket1.maxSize, m1);
                  bound2 = increase(bucket2.maxSize, m2);
                } else if (bucket1.chunks > bucket2.chunks && bucket2.chunks > bucket3.chunks) {
                  bound1 = decrease(bucket1.maxSize, m1);
                  bound2 = decrease(bucket2.maxSize, m2);
                } else if (bucket1.chunks < bucket2.chunks && bucket2.chunks > bucket3.chunks) {
                  bound1 = increase(bucket1.maxSize, m1);
                  bound2 = decrease(bucket2.maxSize, m2);
                } else if (bucket1.chunks > bucket2.chunks && bucket2.chunks < bucket3.chunks) {
                  bound1 = decrease(bucket1.maxSize, m1);
                  bound2 = increase(bucket2.maxSize, m2);
                }
              }
              bound2 = Math.max(minBound2, bound2);

              if (bound1 > bound2) {
                bound1 = bound2 / 2;
              }
              return calculateBounds(i + 1, maxIterations, index, type, [bound1, bound2, bucket3.maxSize]);
            }
          }
          return [newBound(-1, -1, Subtask.DEFAULT_FLUSH_SIZE)];
        })
        .catch((e) => {
          log.error(e);
          return [newBound(-1, -1, Subtask.DEFAULT_FLUSH_SIZE)];
        });

    return Promise.reduce(
        filtered(allIndices, loadedFilters.index)
        .reduce((result, filteredIndex) => result.concat(filtered(getTypesFromMappings(filteredIndex.mappings), loadedFilters.type).map((filteredType) => newTypeInfo(filteredIndex, filteredType))), [])
        .map((typeInfo) =>
            initialBounds(typeInfo.index, typeInfo.type)
            .then((bounds) => calculateBounds(0, 10, typeInfo.index, typeInfo.type, bounds))
            .then((bounds) => bounds.map((bound) => newSubtask(typeInfo.index, typeInfo.type, bound.minSize, bound.maxSize, bound.flushSize)))),
        (result, subtasks) => result.concat(subtasks), []);
  };

  /**
   * Wipe existing backlog and create new backlog based on provided task and completed subtasks
   * @param taskId
   * @param task
   * @returns {Promise.<TResult>}
   */
  self.buildBacklog = (taskId, task) => {
    task = Task.coerce(task);
    const taskSource = createEsClient(task.source);

    return Task.validateId(taskId)
    .then(() => self.clearBacklog(taskId))
    .then(() => Promise.reduce([
      generateIndexSubtasks(taskSource, task),
      generateTemplateSubtasks(taskSource, task),
      generateDocumentSubtasks(taskSource, taskId, task)
    ], (allSubtasks, stepSubtasks) => allSubtasks.concat(stepSubtasks), []))
    .then((potentialSubtasks) => {
      log.info(
          `${potentialSubtasks.length} potential subtasks found`
      );

      return self.getCompleted(taskId)
      .then((completedSubtasks) => {
        log.info(`${completedSubtasks.length} completed subtasks exist`);
        const unfinished = potentialSubtasks.filter((potential) => !_.find(completedSubtasks, potential));

        log.info(`${unfinished.length} unfinished subtasks remain`);
        return unfinished;
      });
    })
    .then((allSubtasks) => Promise.map(allSubtasks, (subtask) => self.addCount(taskSource, subtask), {concurrency: 10}))
    .then((allSubtasks) => self.queue(taskId, allSubtasks));
  };

  /**
   * Clear backlog
   *
   * @returns {Promise.<TResult>}
   */
  self.clearBacklog = (taskId) => Task.validateId(taskId)
  .then(() => log.info(
      `clearing existing backlog for task: '${taskId}'`
  ))
  .then(() => redis.del(Task.backlogQueueKey(taskId)))
  .then(() => redis.del(Task.backlogHSetKey(taskId)));

  /**
   * Returns all backlog jobs and their counts
   *
   * @returns {Promise.<TResult>}
   */
  self.getBacklog = (taskId) => Task.validateId(taskId)
  .then(() => redis.hgetall(Task.backlogHSetKey(taskId)))
  .then((jobsAndCounts) =>// ioredis returns an object where the keys are the hash fields and the values are the hash values
      _.map(jobsAndCounts, (count, subtaskID) => Subtask.createFromID(subtaskID, count)));

  /**
   * Get total docs in backlog
   *
   * @returns {Promise.<TResult>}
   */
  self.countBacklog = (taskId) => Task.validateId(taskId)
  .then(() => redis.hvals(Task.backlogHSetKey(taskId)))
  .then((counts) => counts.reduce((total, count) => total + parseInt(count), 0));

  /**
   * Clear any completed subtasks
   *
   * @returns {Promise.<TResult>}
   */
  self.clearCompleted = (taskId) => Task.validateId(taskId)
  .then(() => redis.del(Task.completedKey(taskId)));

  /**
   * Returns all completed jobs and their counts
   *
   * @returns {Promise.<TResult>}
   */
  self.getCompleted = (taskId) => Task.validateId(taskId)
  .then(() => redis.hgetall(Task.completedKey(taskId)))
  .then((jobsAndCounts) =>// ioredis returns an object where the keys are the hash fields and the values are the hash values
      _.map(jobsAndCounts, (count, subtaskID) => Subtask.createFromID(subtaskID, count)));

  /**
   * Get total docs completed
   *
   * @returns {Promise.<TResult>}
   */
  self.countCompleted = (taskId) => Task.validateId(taskId)
  .then(() => redis.hvals(Task.completedKey(taskId)))
  .then((counts) => counts.reduce((total, count) => total + parseInt(count), 0));

  /**
   * Clear the progress of a given subtask within a task
   * @param taskId
   * @param subtask
   * @returns {*|{arity, flags, keyStart, keyStop, step}}
   */
  self.removeProgress = (taskId, subtask) => Task.validateId(taskId)
  .then(() => redis.hdel(Task.progressKey(taskId), JSON.stringify(subtask)));

  /**
   * Update the progress of a given subtask within a task
   * @param taskId
   * @param subtask
   * @param progress
   * @returns {*|{arity, flags, keyStart, keyStop, step}}
   */
  self.updateProgress = (taskId, subtask, progress) => {
    progress = Progress.coerce(progress);
    progress.lastModified = moment().toISOString();

    return Task.validateId(taskId)
    .then(() => redis.hset(Task.progressKey(taskId), JSON.stringify(Subtask.coerce(subtask)), JSON.stringify(progress)));
  };

  /**
   * Get the progress of a given subtask within a task
   * @param taskId
   * @param subtask
   * @returns {Promise.<TResult>|*}
   */
  self.getProgress = (taskId, subtask) => Task.validateId(taskId)
  .then(() => redis.hget(Task.progressKey(taskId), JSON.stringify(Subtask.coerce(subtask))))
  .then((progress) => JSON.parse(progress));
};

module.exports = Subtasks;