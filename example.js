const Chillastic = require('./index'); // Replace with 'require('chillastic')' if you're outside of this repo
const _          = require('lodash');

const REDIS_HOST             = 'redis';
const REDIS_PORT             = 6379;
const CHILL_PORT_LOWER_LIMIT = 7000;
const CHILL_PORT_UPPER_LIMIT = 10000;
//const CHILL_PORT             = _.random(CHILL_PORT_LOWER_LIMIT, CHILL_PORT_UPPER_LIMIT);
const CHILL_PORT             = 8080;

const chillastic = Chillastic(REDIS_HOST, REDIS_PORT, CHILL_PORT);

// Start it up!
chillastic.run();