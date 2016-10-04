const _        = require('lodash');
const Promise  = require('bluebird');
const Compiler = require('./compiler');
const ObjectId = require('../models/objectId');
const config   = require('../../config/index');

const log      = config.log;
const compiler = new Compiler();

const Mutators = function (redisClient) {
  const self  = this;
  const redis = redisClient;

  const getNamespacedKey = (namespace) => `${namespace}.${Mutators.NAME_KEY}`;

  const validate = (mutatorSrc) => {
    if (!_.isString(mutatorSrc)) {
      throw new Error('mutatorSrc must be string');
    }

    const mutator = compiler.compile(mutatorSrc);
    if (!_.isString(mutator.type)) {
      throw new Error('Mutator type string not provided');
    } else if (!_.includes(Mutators.TYPES, mutator.type)) {
      throw new Error(`Mutator type '${mutator.type}' not one of: [${_.join(Mutators.TYPES, ',')}]`);
    }

    if (!_.isFunction(mutator.predicate)) {
      throw new Error('Mutator predicate() not provided');
    }

    if (!_.isFunction(mutator.mutate)) {
      throw new Error('Mutator mutate() not provided');
    }
  };

  /**
   * Get all mutator ids for a given namespace
   *
   * @param namespace
   * @returns {*|Promise.<TResult>}
   */
  self.getIds = (namespace) => redis.hkeys(getNamespacedKey(namespace));

  /**
   * Add a mutator
   *
   * @param objectId
   * @param mutatorSrc
   * @returns {*|Promise.<TResult>}
   */
  self.add = (objectId, mutatorSrc) =>
      ObjectId.coerce(objectId).validate()
      .then(() => validate(mutatorSrc))
      .then(() => self.exists(objectId))
      .then((exists) => {
        if (exists) {
          throw new Error(`Mutator '${objectId.namespace}/${objectId.id}' exists, delete first.`);
        }
        return redis.hset(getNamespacedKey(objectId.namespace), objectId.id, mutatorSrc);
      });

  /**
   * Remove a mutator by objectId
   * @param objectId
   * @returns {Promise.<TResult>}
   */
  self.remove = (objectId) =>
      ObjectId.coerce(objectId).validate()
      .then(() => redis.hdel(getNamespacedKey(objectId.namespace), objectId.id));

  /**
   * Remove all filters by namespace
   * @param objectId
   * @returns {Promise.<TResult>}
   */
  self.removeAllNamespacedBy = (objectId) =>
      ObjectId.coerce(objectId).validate()
      .then(() => self.getIds(objectId.namespace))
      .then((ids) => _.map(ids, (id) => redis.hdel(getNamespacedKey(objectId.namespace), id)));

  /**
   * Return TRUE if a mutator exists in the system based on it's objectId
   * @param objectId
   * @returns {*|{arity, flags, keyStart, keyStop, step}}
   */
  self.exists = (objectId) =>
      ObjectId.coerce(objectId).validate()
      .then((id) => redis.hexists(getNamespacedKey(id.namespace), id.id));

  /**
   * Return TRUE if a mutator exists in the system based on it's id
   * @param taskName
   * @param mutators
   * @returns {*|{arity, flags, keyStart, keyStop, step}}
   */
  self.load = (taskName, mutators) =>
      !_.isObject(mutators) || !_.isArray(mutators.actions) ? Promise.resolve({}) :
          Promise.map(mutators.actions, (action) => {
            const id     = new ObjectId({namespace: _.isString(action.namespace) ? action.namespace : taskName, id: action.id});
            id.arguments = action.arguments || mutators.arguments;
            return id.validate()
            .then(() => redis.hget(getNamespacedKey(id.namespace), id.id))
            .then((src) => _.assign(compiler.compile(src), id));
          })
          .then((modules) => Promise.reduce(modules, (loadedModules, module) => {
            if (!_.isArray(loadedModules[module.type])) {
              loadedModules[module.type] = [];
            }
            log.info(`adding mutator [${module.namespace}:${module.id}] [type ${module.type}]`);
            loadedModules[module.type].push(module);
            return loadedModules;
          }, {}));

  self.ensureMutatorsExist = (taskName, mutators) =>
      !_.isObject(mutators) || !_.isArray(mutators.actions) ? Promise.resolve() :
          Promise.map(mutators.actions, (action) => {
            const id     = new ObjectId({namespace: _.isString(action.namespace) ? action.namespace : taskName, id: action.id});
            id.arguments = action.arguments || mutators.arguments;
            return id.validate()
            .then(() => redis.hget(getNamespacedKey(id.namespace), id.id))
            .then((src) => src ? _.assign(compiler.compile(src), id) : Promise.reject(new Error(`Src for mutator id ${id.id} not found`)));
          });

};

Mutators.NAME_KEY = 'mutators';
Mutators.TYPES = [
  'data',
  'template',
  'index'
];

module.exports = Mutators;