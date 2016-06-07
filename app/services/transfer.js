const Promise = require('bluebird');
const _       = require('lodash');
const fs      = require('fs');
const path    = require('path');
const config  = require('../../config/index');
const log     = config.log;

const FLUSH_SIZE = 450;

const MUTATOR_TYPES = [
  'data',
  'template',
  'index'
];

const MAX_FLUSH_RETRY = 5;
const MIN_RETRY_WAIT  = 2 * 1000;
const MAX_RETRY_WAIT  = 7 * 1000;

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
  let source         = null;
  let dest           = null;

  let flushRetryCount = 0;
  const bulkQueue     = [];
  let mutators        = {};
  let queueSummary    = {
    tick:        0,
    transferred: 0,
    scrolled:    0,
    errors:      0
  };

  self.source = sourceEs;
  source      = sourceEs;
  self.dest   = destEs;
  dest        = destEs;

  /**
   * Flushes any queued bulk inserts
   *
   * @returns {Promise.<TResult>}
   */
  const flushQueue = ()=> {
    if (bulkQueue.length > 0) {
      const bulkBody = [];
      while (bulkQueue.length > 0) {
        bulkBody.push(bulkQueue.shift());
      }

      queueSummary.tick = 0;

      return dest.bulk({body: bulkBody}).then((results)=> {
        // log.info('response', JSON.stringify(results, null, 2));

        if (results.errors && results.errors > 0) {
          return handleBulkErrors(results, bulkBody);
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
   * @param body
   */
  self.transferData = (targetIndex, targetType, body) => {
    queueSummary = {
      tick:        0,
      transferred: 0,
      scrolled:    0,
      errors:      0
    };

    if (!_.isString(targetIndex) || targetIndex.length === 0) {
      throw new Error('targetIndex must be string with length');
    }

    if (!_.isString(targetType) || targetType.length === 0) {
      throw new Error('targetType must be string with length');
    }

    if (body && !_.isObject(body)) {
      throw new Error('if provided, body must be an object');
    }

    flushRetryCount = 0;

    return source.search({
      index:  targetIndex,
      type:   targetType,
      scroll: '1m',
      body:   body,
      size:   40
    }).then(function scrollAndGetData(response) {

      // log.info('response', JSON.stringify(response, null, 2));
      // log.info('size', response.hits.hits.length);

      const documents = [];
      response.hits.hits.forEach((hit)=> {
        documents.push(hit);
        queueSummary.scrolled++;
      });

      return putData(mutate(documents, 'data')).then(()=> {
        if (response.hits.total !== queueSummary.scrolled) {
          return source.scroll({
            scroll_id: response._scroll_id,
            scroll:    '1m'
          }).then((inner_response)=> {
            log.debug('scrolling: ', queueSummary);
            return scrollAndGetData(inner_response);
          });
        } else {
          return flushQueue().then(()=> {
            log.debug('transfer complete: ', queueSummary);
            return queueSummary.transferred;
          });
        }
      });

    }).catch((error)=> {
      log.error('Error during search: ', error);
      return Promise.reject(error);
    });

  };


  /**
   * Add mutator
   *
   * @param mutator
   * @param args
   */
  const addMutator = (mutator, args)=> {
    if (!_.isString(mutator.type)) {
      throw new Error('Mutator type string not provided');
    } else if (!_.includes(MUTATOR_TYPES, mutator.type)) {
      throw new Error(`Mutator type '${mutator.type}' not one of: [${_.join(MUTATOR_TYPES, ',')}]`);
    }

    if (!_.isFunction(mutator.predicate)) {
      throw new Error('Mutator predicate() not provided');
    }

    if (!_.isFunction(mutator.mutate)) {
      throw new Error('Mutator mutate() not provided');
    }

    if (!_.isArray(mutators[mutator.type])) {
      mutators[mutator.type] = [];
    }

    log.info(`adding mutator type ${mutator.type}`);

    mutator.arguments = args;
    mutators[mutator.type].push(mutator);
  };

  /**
   * Load all mutator modules in a specified path
   *
   * @param mutatorPath
   * @param mutatorArgs
   */
  const loadMutators = (mutatorPath, mutatorArgs)=> {
    let files   = [];
    const isDir = fs.lstatSync(mutatorPath).isDirectory();

    if (isDir) {
      files = fs.readdirSync(mutatorPath);
      files = _.map(files, (file)=> {
        return `${mutatorPath}/${file}`;
      });
    } else {
      files = [mutatorPath];
    }

    files = _.filter(files, (file)=> {
      return path.extname(file) === '.js';
    });

    if (files.length === 0) {
      throw new Error(`No .js file(s) at: ${mutatorPath}`);
    }

    _.map(files, (file)=> {
      log.info(`loading mutator from file: ${file}`);
      addMutator(require(file), mutatorArgs);
    });
  };

  /**
   * Apply appropriate mutations each original object
   *
   * @param originals
   * @param type
   * @returns {*}
   */
  const mutate = (originals, type) => {
    if (_.isArray(mutators[type]) && mutators[type].length > 0) {
      return _.reduce(originals, (result, original)=> {
        for (let i = 0; i < mutators[type].length; i++) {
          if (mutators[type][i].predicate(original, mutators[type][i].arguments)) {
            const mutated = mutators[type][i].mutate(original, mutators[type][i].arguments);

            if (!_.isUndefined(mutated) && !_.isNull(mutated) && !_.isEmpty(mutated)) {
              original = mutated;
            }
          }
        }

        result.push(original);
        return result;
      }, []);
    }

    return originals;
  };

  /**
   * Fetch templates from source ES
   *
   * @param targetTemplates
   * @returns {Promise.<TResult>}
   */
  const getTemplates = (targetTemplates) => {
    if (!_.isString(targetTemplates) || targetTemplates.length === 0) {
      throw new Error('targetTemplates must be string with length');
    }

    return source.indices.getTemplate({
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

  /**
   * Add all provided templates to dest ES
   *
   * @param templates
   * @returns {*|Array}
   */
  const putTemplates = (templates)=> {
    if (!_.isArray(templates)) {
      throw new Error('templates must be an array');
    }

    return Promise.map(templates, (template)=> {
      const name = template.name;
      delete template.name;

      log.info('putting template: ', name);
      return dest.indices.putTemplate({
        name: name,
        body: template
      }).catch((error)=> {
        log.error('Error during put templates: ', error);
        return Promise.reject(error);
      });
    });
  };


  /**
   * Queue docs for upsert into dest elasticsearch
   * @param documents
   * @returns {*}
   */
  const putData = (documents) => {
    _.reduce(documents, docToBulk, bulkQueue);

    if (bulkQueue.length > FLUSH_SIZE) {
      return flushQueue();
    } else {
      return Promise.resolve();
    }
  };

  /**
   * Get mappings from source ES
   *
   * @param indexNames
   * @returns {Promise.<TResult>}
   */
  const getIndices = (indexNames)=> {
    if (!_.isString(indexNames) || indexNames.length === 0) {
      throw new Error('index names must be a string with length');
    }

    return source.indices.get({
      index:          indexNames,
      allowNoIndices: true
    }).then((indices)=> {
      return _.reduce(indices, (result, index, name)=> {
        log.info('got index: ', name);
        index.name = name;
        result.push(index);
        return result;
      }, []);
    }).catch((error)=> {
      log.error('Error during index get: ', error);
      return Promise.reject(error);
    });
  };

  /**
   * Push index configurations (not data) to destination
   *
   * @param indices
   * @returns {Array|*}
   */
  const putIndices = (indices)=> {
    if (!_.isArray(indices)) {
      throw new Error('indices must be an array');
    }

    return Promise.map(indices, (index)=> {
      const name = index.name;
      delete index.name;

      log.info('creating index: ', name);

      if (!_.isString(name)) {
        log.error('bad index object: ', JSON.stringify(index, null, 2));
        throw new Error('name must be defined');
      }

      return dest.indices.create({
        index: name,
        body:  index
      }).catch((error)=> {
        log.error(`Error during index (${name}) put: `, error);
        return Promise.reject(error);
      });
    });
  };

  /**
   * Add provided document to bulk queue as upsert.
   *
   * @param queue
   * @param document
   * @returns {*}
   */
  const docToBulk = (queue, document)=> {
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
   * If any errors are detected in the transfer, they are handled here for possible recovery
   *
   * @param results
   * @param bulkBody
   * @returns {*}
   */
  const handleBulkErrors = (results, bulkBody)=> {
    const unrecoverableErrors = [];

    _.forEach(results.items, (item, id)=> {

      // There is only ever one key per item in the response
      const actionType = Object.keys(item)[0];

      // If there is a rejection error, we're just overloading the ingress of the destination
      // Re-add the relevant record to the queue and try again later
      if (item[actionType].error) {
        if (item[actionType].error.type === 'es_rejected_execution_exception') {
          // log.warn('Recoverable error during batch, retrying later', item[actionType].error);

          // Action is found at 2 x id
          bulkQueue.push(bulkBody[id * 2]);

          // Data is 2 x id + 1
          bulkQueue.push(bulkBody[(id * 2) + 1]);
        } else {
          log.error('Unrecoverable error during batch', item[actionType]);
          log.error('Source action: ', bulkBody[id * 2]);
          log.error('Source data: ', JSON.stringify(bulkBody[(id * 2) + 1]));
          unrecoverableErrors.push(item[actionType]);
        }

        queueSummary.errors++;
      } else {
        queueSummary.transferred++;
        queueSummary.tick++;
      }
    });

    if (unrecoverableErrors.length > 0) {
      return Promise.reject(JSON.stringify(unrecoverableErrors, null, 2));
    } else if (flushRetryCount > MAX_FLUSH_RETRY) {
      return Promise.reject('Exceeded max flush retries');
    } else {
      flushRetryCount++;

      return new Promise((resolve)=> {
        const timeout = _.random(MIN_RETRY_WAIT, MAX_RETRY_WAIT);

        log.warn(`Recoverable errors detected, sleeping ${timeout}msec and retrying...`);

        setTimeout(()=> {
          log.warn(`Flush retry ${flushRetryCount}`);
          resolve(flushQueue());
        }, timeout);
      });
    }
  };

  self.getTemplates = getTemplates;
  self.putTemplates = putTemplates;

  self.getIndices = getIndices;
  self.putIndices = putIndices;

  self.clearMutators = ()=> {
    mutators = {};
  };

  self.getMutators = ()=> {
    return _.cloneDeep(mutators);
  };

  self.loadMutators = loadMutators;
  self.addMutator   = addMutator;
  self.mutate       = mutate;

  self.setUpdateCallback = (callback)=> {
    updateCallback = callback;
  };

  self.transferIndices = (indicesNames)=> {
    return getIndices(indicesNames).then((indices)=> {
      return putIndices(mutate(indices, 'index'));
    });
  };

  self.transferTemplates = function (templateNames) {
    return getTemplates(templateNames).then((templates)=> {
      return putTemplates(mutate(templates, 'template'));
    });
  };

  self.handleBulkErrors = handleBulkErrors;

  return self;
};


module.exports = Transfer;

