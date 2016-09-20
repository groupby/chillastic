const _            = require('lodash');
const bunyan       = require('bunyan');
const PrettyStream = require('bunyan-prettystream');
const prettyStdOut = new PrettyStream({mode: 'dev'});
prettyStdOut.pipe(process.stdout);

const utils           = require('./utils');
const MAX_PORT_NUMBER = 65535;

const createLogger = () => {
  return bunyan.createLogger({
    name:    currentConfig.FRAMEWORK_NAME,
    streams: [
      {
        type:   'raw',
        level:  currentConfig.logLevel,
        stream: prettyStdOut
      }
    ]
  });
};

const setLogLevel = (level) => {
  currentConfig.logLevel = level;
  currentConfig.log = createLogger();
};

const configureRedis = (host, port) => {
  if (!utils.isNonZeroString(host)) {
    throw new Error('redis host must be string with length');
  }

  if (!_.isInteger(port) || port < 1 || port > MAX_PORT_NUMBER) {
    throw new Error('redis port must be integer from 1-65535');
  }

  currentConfig.redis.host = host;
  currentConfig.redis.port = port;
};

const setPort = (port) => {
  if (!_.isInteger(port) || port < 1 || port > MAX_PORT_NUMBER) {
    throw new Error('port must be integer from 1-65535');
  }

  currentConfig.port = port;
};

const DEFAULT_CONFIG = {
  FRAMEWORK_NAME: 'chillastic',
  logLevel:       'info',
  elasticsearch:  {
    logLevel: 'warn'
  },
  redis: {
    host: null,
    port: null
  },
  port: 8080
};

const currentConfig = {};
_.defaultsDeep(currentConfig, DEFAULT_CONFIG);

const log = createLogger();

currentConfig.setLogLevel = setLogLevel;
currentConfig.configureRedis = configureRedis;
currentConfig.setPort = setPort;
currentConfig.log = log;
currentConfig.jsonIndent = 2;
currentConfig.numDigits = 2;

module.exports = currentConfig;