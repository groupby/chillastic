const _        = require('lodash');
const Promise  = require('bluebird');
const Compiler = require('./compiler');
const ObjectId = require('../models/objectId');
const config   = require('../../config/index');

const log      = config.log;
const compiler = new Compiler();

const Filters    = function (redisClient) {
  const self  = this;
  const redis = redisClient;

  const getNamespacedKey = (namespace)=> `${namespace}.${Filters.NAME_KEY}`;

  const validate = (filterSrc)=> {
    if (!_.isString(filterSrc)) {
      throw new Error('filterSrc must be string');
    }

    const filter = compiler.compile(filterSrc);
    if (!_.isString(filter.type)) {
      throw new Error('Filter type string not provided');
    } else if (!_.includes(Filters.TYPES, filter.type)) {
      throw new Error(`Filter type '${filter.type}' not one of: [${_.join(Filters.TYPES, ',')}]`);
    }

    if (!_.isFunction(filter.predicate)) {
      throw new Error('Filter predicate() not provided');
    }
  };

  /**
   * Get all filter ids for a given namespace
   *
   * @param namespace
   * @returns {*|Promise.<TResult>}
   */
  self.getIds = (namespace)=> redis.hkeys(getNamespacedKey(namespace));

  /**
   * Add a filter
   *
   * @param objectId
   * @param filterSrc
   * @returns {*|Promise.<TResult>}
   */
  self.add = (objectId, filterSrc)=>
      ObjectId.coerce(objectId).validate()
      .then(()=> validate(filterSrc))
      .then(()=> self.exists(objectId))
      .then((exists) => {
        if (exists) {
          throw new Error(`Filter '${objectId.namespace}/${objectId.id}' exists, delete first.`);
        }
        return redis.hset(getNamespacedKey(objectId.namespace), objectId.id, filterSrc);
      });

  /**
   * Remove a filter by objectId
   * @param objectId
   * @returns {Promise.<TResult>}
   */
  self.remove = (objectId) =>
      ObjectId.coerce(objectId).validate()
      .then((id)=> redis.hdel(getNamespacedKey(objectId.namespace), objectId.id));

  /**
   * Remove all filters by namespace
   * @param objectId
   * @returns {Promise.<TResult>}
   */
  self.removeAllNamespacedBy = (objectId) =>
      ObjectId.coerce(objectId).validate()
      .then(()=> self.getIds(objectId.namespace))
      .then((ids)=> _.map(ids, (id)=> redis.hdel(getNamespacedKey(objectId.namespace), id)));

  /**
   * Return TRUE if a filter exists in the system based on it's objectId
   * @param objectId
   * @returns {*|{arity, flags, keyStart, keyStop, step}}
   */
  self.exists = (objectId) =>
      ObjectId.coerce(objectId).validate()
      .then((id)=> redis.hexists(getNamespacedKey(id.namespace), id.id));

  /**
   * Return TRUE if a filter exists in the system based on it's id
   * @param taskName
   * @param filters
   * @returns {*|{arity, flags, keyStart, keyStop, step}}
   */
  self.load = (taskName, filters) =>
      Promise.mapSeries(filters.actions, (action)=> {
        const id     = new ObjectId({namespace: _.isString(action.namespace) ? action.namespace : taskName, id: action.id});
        id.arguments = action.arguments || filters.arguments;
        return id;
      })
      .then((objectIds)=> Promise.mapSeries(objectIds, (objectId)=>
          objectId.validate()
          .then(()=> redis.hget(getNamespacedKey(objectId.namespace), objectId.id))
          .then((src)=> _.assign(compiler.compile(src), objectId))))
      .then((modules)=> Promise.reduce(modules, (loadedModules, module)=> {
        if (!_.isArray(loadedModules[module.type])) {
          loadedModules[module.type] = [];
        }

        log.info(`adding filter [${module.namespace}:${module.id}] [type ${module.type}]`);
        loadedModules[module.type].push(module);
        return loadedModules;
      }, {}));
};
Filters.NAME_KEY = 'filters';
Filters.TYPES    = [
  'index',
  'type'
];

module.exports = Filters;