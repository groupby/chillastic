const Manager           = require('./manager');
const Filters           = require('./filters');
const Mutators          = require('./mutators');
const Tasks             = require('./tasks');
const Subtasks          = require('./subtasks');
const Worker            = require('./worker');
const config            = require('../../config');
const createRedisClient = require('../../config/redis');

const redis = createRedisClient(config.redis.host, config.redis.port);

module.exports = {
  worker:   new Worker(redis),
  manager:  new Manager(redis),
  mutators: new Mutators(redis),
  filters:  new Filters(redis),
  tasks:    new Tasks(redis),
  subtasks: new Subtasks(redis),
  redis:    redis
};