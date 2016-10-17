const Chillastic = require('./index'); // Replace with 'require('chillastic')' if you're outside of this repo

const REDIS_HOST = 'localhost';
const REDIS_PORT = 6379;
const CHILL_PORT = 8080;

const chillastic = Chillastic(REDIS_HOST, REDIS_PORT, CHILL_PORT);

// Start it up!
chillastic.run();