/*eslint no-process-env: "off" */
const express = require('express');
const _       = require('lodash');
const config  = require('./config');
const log     = require('./config').log;

const create = (redisHost, redisPort, port)=> {
  config.configureRedis(redisHost, redisPort);

  if (!_.isUndefined(port) && !_.isNull(port)) {
    config.setPort(port);
  }

  const app    = express();
  const server = require('http').createServer(app);
  require('./config/express')(app);
  require('./app/routes')(app);

  app.config   = config;
  app.services = require('./app/services');

  app.run = () => {
    app.services.manager.setRunning(true);
    server.listen(config.port, () => {
      log.info(`watchdog server listening on port ${config.port}`);
    });
  };

  app.stop = () => {
    app.services.worker.setRunning(false);
    server.close();
  };

  return app;
};

module.exports = create;

