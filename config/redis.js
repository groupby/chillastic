'use strict';
var Redis = require('ioredis');
var redis = new Redis({
  port: 6379,
  host: 'localhost',
  db:   10 // So it can't possibly conflict with the recommendation cache
});

export default redis;