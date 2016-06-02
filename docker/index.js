const Chillastic = require('chillastic');

const chillastic = Chillastic(process.env.REDIS_HOST, parseInt(process.env.REDIS_PORT));
chillastic.run();