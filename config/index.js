const _            = require('lodash');
const bunyan       = require('bunyan');
const PrettyStream = require('bunyan-prettystream');
const prettyStdOut = new PrettyStream({mode: 'dev'});
prettyStdOut.pipe(process.stdout);

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
  })
};

const setLogLevel = (level) => {
  currentConfig.logLevel = level;
  currentConfig.log = createLogger();
};

const DEFAULT_CONFIG = {
  setLogLevel:     setLogLevel,
  FRAMEWORK_NAME: 'chillastic',
  logLevel:       'info',
  elasticsearch:  {
    logLevel: 'warn'
  }
};

const currentConfig = {};
_.defaultsDeep(currentConfig, DEFAULT_CONFIG);

currentConfig.log = createLogger();

module.exports = currentConfig;