/*eslint no-process-env: "off" */
const Redis   = require('ioredis');
Redis.Promise = require('bluebird');

const createClient = (host, port) => new Redis({host, port, db: 10});

module.exports = createClient;