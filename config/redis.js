'use strict';
var Redis = require('ioredis');
var redis = new Redis({
  port: 6379,
  host: process.env.REDIS_HOST || 'localhost',
  db:   10
});

export default redis;