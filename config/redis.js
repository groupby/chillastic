/*eslint no-process-env: "off" */
const Redis = require('ioredis');
const redis = new Redis({
  port: 6379,
  host: process.env.REDIS_HOST || 'localhost',
  db:   10
});

module.exports = redis;