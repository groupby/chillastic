const Chillastic = require('./index');
const _          = require('lodash');

const chillastic = Chillastic('localhost', 6379, _.random(7000, 60000));
chillastic.run();

// chillastic.services.manager.addTask('default', {})

