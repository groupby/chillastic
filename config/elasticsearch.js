const config        = require('../config');
const Promise       = require('bluebird');
const elasticsearch = require('elasticsearch');
const bunyan        = require('bunyan');
const PrettyStream  = require('bunyan-prettystream');
const prettyStdOut  = new PrettyStream({mode: 'dev'});
prettyStdOut.pipe(process.stdout);

const LogToBunyan = function () {
  const self = this;
  const bun  = bunyan.createLogger({
    name:   `${config.FRAMEWORK_NAME}-es`,
    type:   'raw',
    level:  config.elasticsearch.logLevel,
    stream: prettyStdOut
  });

  self.error   = bun.error.bind(bun);
  self.warning = bun.warn.bind(bun);
  self.info    = bun.info.bind(bun);
  self.debug   = bun.debug.bind(bun);
  self.trace   = (method, requestUrl, body, responseBody, responseStatus) => {
    bun.trace({
      method:         method,
      requestUrl:     requestUrl,
      body:           body,
      responseBody:   responseBody,
      responseStatus: responseStatus
    });
  };
  self.close   = () => {};
};

const createEsClient = (host, apiVersion) => {
  return new elasticsearch.Client({
    host:       host,
    apiVersion: apiVersion,
    log:        LogToBunyan,
    defer:      function () {
      let resolve = null;
      let reject  = null;

      const promise = new Promise((res, rej) => {
        resolve = res;
        reject  = rej;
      });
      return {
        resolve: resolve,
        reject:  reject,
        promise: promise
      };
    }
  });
};

module.exports = createEsClient;