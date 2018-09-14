const _          = require('lodash');
const Promise    = require('bluebird');
const HttpStatus = require('http-status');
const Subtask    = require('../models/subtask');
const config     = require('../../config/index');

const log = config.log;

const MAX_FLUSH_RETRY         = 5;
const MIN_RETRY_WAIT_MS       = 2000;
const MAX_RETRY_WAIT_MS       = 7000;
const BULK_REQUEST_TIMEOUT_MS = 3600000;

/**
 * Transfer constructor
 *
 * @param sourceEs
 * @param destEs
 * @returns {Transfer}
 * @constructor
 */
const Transfer = function (sourceEs, destEs) {
  const self         = this;
  let updateCallback = null;

  let flushRetryCount = 0;
  const bulkQueue     = [];
  let mutatorsByType  = {};
  let queueSummary    = {
    tick:        0,
    transferred: 0,
    scrolled:    0,
    errors:      0
  };

  self.source = sourceEs;
  self.dest = destEs;

  /**
   * Flushes any queued bulk inserts
   *
   * @returns {Promise.<TResult>}
   */
  const flushQueue = () => {
    if (bulkQueue.length > 0) {
      const bulkBody = [];
      while (bulkQueue.length > 0) {
        bulkBody.push(bulkQueue.shift());
      }

      queueSummary.tick = 0;

      return self.dest.bulk({refresh: true, body: bulkBody, requestTimeout: BULK_REQUEST_TIMEOUT_MS})
      .then((results) => {
        log.trace('response: %s', JSON.stringify(results, null, config.jsonIndent));

        if (results.errors && results.errors > 0) {
          return self.handleBulkErrors(results, bulkBody);
        } else {
          queueSummary.transferred += results.items.length;
          queueSummary.tick = results.items.length;

          if (_.isFunction(updateCallback)) {
            updateCallback(queueSummary);
          }

          log.debug('flush complete: ', queueSummary);
          return Promise.resolve();
        }
      });
    } else {
      return Promise.resolve();
    }
  };

  self.scroll = (response, retries = 0) =>
      self.source.scroll({
        scroll_id: response._scroll_id,
        scroll:    '1h'
      }).catch(() => retries > 3 ? Promise.reject(new Error(`can't scroll: ${response._scroll_id}`)) : self.scroll(response, retries + 1));

  self.search = (request, retries = 0) =>
      self.source.search(request)
      .catch(() => retries > 3 ? Promise.reject(new Error(`can't search: ${JSON.stringify(request)}`)) : self.search(request, retries + 1));

  /**
   * Scan and scroll to get data from specific index and type in source ES.
   *
   * Apply any relevant mutations to the data.
   *
   * Queue it for sending to destination.
   *
   * Optionally provide query to restrict the data to be retrieved.
   * @param targetIndex
   * @param targetType
   * @param flushSize
   * @param minSize
   * @param maxSize
   */
  self.transferData = (targetIndex, targetType, flushSize, minSize, maxSize) => {
    queueSummary = {
      tick:        0,
      transferred: 0,
      scrolled:    0,
      errors:      0
    };

    if (!_.isString(targetIndex) || targetIndex.length === 0) {
      throw new Error('targetIndex must be string with length');
    } else if (!_.isString(targetType) || targetType.length === 0) {
      throw new Error('targetType must be string with length');
    }

    flushRetryCount = 0;

    const scrollAndGetData = (response) => {
      const documents = [];
      response.hits.hits.forEach((hit) => {
        documents.push(hit);
        queueSummary.scrolled++;
      });

      return putData(self.mutate(documents, 'data'), flushSize)
      .then(() => {
        if (response.hits.total !== queueSummary.scrolled) {
          return self.scroll(response)
          .then((inner_response) => {
            log.debug('scrolling: ', queueSummary);
            return scrollAndGetData(inner_response);
          });
        } else {
          return flushQueue()
          .then(() => {
            log.debug('transfer complete: ', queueSummary);
            return queueSummary.transferred;
          });
        }
      });
    };

    return self.search(Subtask.createQuery(targetIndex, targetType, flushSize, minSize, maxSize))
    .then(scrollAndGetData)
    .catch((error) => {
      log.error('Error during search: ', error);
      return Promise.reject(error);
    });
  };

  /**
   * Queue docs for upsert into dest elasticsearch
   * @param documents
   * @param flushSize
   * @returns {*}
   */
  const putData = (documents, flushSize) => {
    documents.reduce(docToBulk, bulkQueue);
    if (bulkQueue.length > flushSize) {
      return flushQueue();
    } else {
      return Promise.resolve();
    }
  };

  /**
   * Add provided document to bulk queue as upsert.
   *
   * @param queue
   * @param document
   * @returns {*}
   */
  const docToBulk = (queue, document) => {
    queue.push({
      update: {
        _index: document._index,
        _type:  document._type,
        _id:    document._id
      }
    });
    queue.push({
      doc:           document._source,
      doc_as_upsert: true
    });

    return queue;
  };

  /**
   * Add all provided templates to dest ES
   *
   * @param templates
   * @returns {*|Array}
   */
  self.putTemplates = (templates) => {
    if (!_.isArray(templates)) {
      throw new Error('templates must be an array');
    }

    return Promise.map(templates, (template) => {
      const name = template.name;
      delete template.name;

      log.info('putting template: ', name);
      return self.dest.indices.putTemplate({
        name: name,
        body: template
      })
      .catch((e) => {
        log.error('Error during put templates: %s', JSON.stringify(e));
        return Promise.reject(e);
      });
    });
  };

  /**
   * Push index configurations (not data) to destination
   *
   * @param indices
   * @returns {Array|*}
   */
  self.putIndices = (indices) => {
    if (!_.isArray(indices)) {
      throw new Error('indices must be an array');
    }

    return Promise.map(indices, (index) => {
      const name = index.name;
      delete index.name;
      log.info('creating index: ', name);

      // clean settings
      if (index.settings && index.settings.index) {
        const indexSettings = index.settings.index;
        delete indexSettings.uuid;
        delete indexSettings.creation_date;
        delete indexSettings.provided_name;
        if (indexSettings.version) {
          delete indexSettings.version.created;
        }
      }

      if (!_.isString(name)) {
        log.error('bad index object: ', JSON.stringify(index, null, config.jsonIndent));
        throw new Error('name must be defined');
      }

      return self.dest.indices.create({
        index: name,
        body:  index
      }).catch((error) => {
        log.error(`Error during index (${name}) put: `, error);
        return Promise.reject(error);
      });
    });
  };

  self.getMutators = () => _.cloneDeep(mutatorsByType);
  self.setMutators = (newMutators) => mutatorsByType = newMutators;
  self.clearMutators = () => mutatorsByType = {};

  /**
   * Apply appropriate mutations each original object
   *
   * @param objs
   * @param type
   * @returns {*}
   */
  self.mutate = (objs, type) => {
    const shouldDrop    = (obj) => _.isUndefined(obj) || _.isNull(obj) || _.isEmpty(obj);
    const mutators      = mutatorsByType[type];
    const applyMutators = (obj) => mutators.reduce((result, mutator) => {
      if (shouldDrop(result)) {
        return null;
      } else if (mutator.predicate(result, mutator.arguments)) {
        return mutator.mutate(result, mutator.arguments);
      } else {
        return result;
      }
    }, obj);
    return !_.isArray(mutators) || mutators.length === 0 ? objs : objs.map(applyMutators).filter((obj) => !shouldDrop(obj));
  };

  self.setUpdateCallback = (callback) => {
    updateCallback = callback;
  };

  self.transferIndices = (indicesNames) =>
      Transfer.getIndices(self.source, indicesNames)
      .then((indices) => self.putIndices(self.mutate(indices, 'index')));

  self.transferTemplates = (templateNames) =>
      Transfer.getTemplates(self.source, templateNames)
      .then((templates) => self.putTemplates(self.mutate(templates, 'template')));

  /**
   * If any errors are detected in the transfer, they are handled here for possible recovery
   *
   * @param results
   * @param bulkBody
   * @param retryTimeout
   * @returns {*}
   */
  self.handleBulkErrors = (results, bulkBody, retryTimeout = null) => {
    const skip                = 2;
    const unrecoverableErrors = [];
    results.items.forEach((item, id) => {
      // There is only ever one key per item in the response
      const actionType = Object.keys(item)[0];

      // If there is a rejection error, we're just overloading the ingress of the destination
      // Re-add the relevant record to the queue and try again later
      if (item[actionType].error) {
        const index = id * skip;
        if (item[actionType].error.type === 'es_rejected_execution_exception') {
          log.warn('Recoverable error during batch, retrying later', item[actionType].error);
          bulkQueue.push(bulkBody[index]);
          bulkQueue.push(bulkBody[index + 1]);
        } else {
          log.error('Unrecoverable error during batch', item[actionType]);
          log.error('Source action: ', bulkBody[index]);
          log.error('Source data: ', JSON.stringify(bulkBody[index + 1]));
          unrecoverableErrors.push(item[actionType]);
        }
        queueSummary.errors++;
      } else {
        queueSummary.transferred++;
        queueSummary.tick++;
      }
    });

    if (unrecoverableErrors.length > 0) {
      return Promise.reject(JSON.stringify(unrecoverableErrors, null, config.jsonIndent));
    } else if (flushRetryCount > MAX_FLUSH_RETRY) {
      return Promise.reject('Exceeded max flush retries');
    } else {
      flushRetryCount++;

      return new Promise((resolve) => {
        const timeout = retryTimeout || _.random(MIN_RETRY_WAIT_MS, MAX_RETRY_WAIT_MS);

        log.warn(`Recoverable errors detected, sleeping ${timeout} msec and retrying...`);

        setTimeout(() => {
          log.warn(`Flush retry ${flushRetryCount}`);
          resolve(flushQueue());
        }, timeout);
      });
    }
  };

  return self;
};

/**
 * Get mappings from source ES
 *
 * @param client
 * @param targetIndices
 * @returns {Promise.<TResult>}
 */
Transfer.getIndices = (client, targetIndices) =>
    !_.isString(targetIndices) || targetIndices.length === 0 ?
        Promise.reject(new Error('targetIndices must be string with length')) :
        client.indices.get({index: targetIndices, allowNoIndices: true})
        .then((response) => _.reduce(response, (result, index, name) => result.concat(_.assign(index, {name})), []))
        .catch((e) => {
          log.error('Error during index get: %s', JSON.stringify(e));
          return Promise.reject(e);
        });

/**
 * Returns an array of the templates found using the elasticsearch multi-index definition.
 *
 * The format is similar to an ES template GET command, but with the name nested in the element.
 *
 * @param client
 * @param targetTemplates
 * @returns {Promise.<T>}
 */
Transfer.getTemplates = (client, targetTemplates) =>
    !_.isString(targetTemplates) || targetTemplates.length === 0 ?
        Promise.reject(new Error('targetTemplates must be string with length')) :
        client.indices.getTemplate({name: targetTemplates})
        .then((templates) =>
            _.reduce(templates, (result, template, name) =>
                    _.size(template.index_patterns.filter((p) => p.startsWith('.'))) === 0 ? result.concat(_.assign(template, {name})) : result,
                []))
        .then((templates) => {
          if (_.size(templates) === 0) {
            log.warn('Templates asked to be copied, but none found');
            return Promise.reject('Templates asked to be copied, but none found');
          } else {
            return templates;
          }
        })
        .catch((error) => {
          if (error.status === HttpStatus.NOT_FOUND) {
            log.warn('Templates asked to be copied, but none found');
            return Promise.reject('Templates asked to be copied, but none found');
          } else {
            return Promise.reject(error);
          }
        });

module.exports = Transfer;

