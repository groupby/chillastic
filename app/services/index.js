const Manager           = require('./manager');
const Worker            = require('./worker');
const config            = require('../../config');
const createRedisClient = require('../../config/redis');

const redis = createRedisClient(config.redis.host, config.redis.port);

module.exports = {
  worker:  new Worker(redis),
  manager: new Manager(redis),
  redis:   redis
};