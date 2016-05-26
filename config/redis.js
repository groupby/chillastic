/*eslint no-process-env: "off" */
const Redis = require('ioredis');

const createClient = (host, port)=>{
  return new Redis({
    port: port,
    host: host,
    db:   10
  });
};

module.exports = createClient;