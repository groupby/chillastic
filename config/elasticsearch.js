const Promise       = require('bluebird');
const elasticsearch = require('elasticsearch');
const sr            = require('sync-request');
const bunyan        = require('bunyan');
const PrettyStream  = require('bunyan-prettystream');
const semver        = require('semver');
const config        = require('../config');

const prettyStdOut = new PrettyStream({mode: 'dev'});
prettyStdOut.pipe(process.stdout);

const LogToBunyan = function () {
  const self = this;
  const bun  = bunyan.createLogger({
    name:   `${config.FRAMEWORK_NAME}-es`,
    type:   'raw',
    level:  config.elasticsearch.logLevel,
    stream: prettyStdOut
  });

  self.error = bun.error.bind(bun);
  self.warning = bun.warn.bind(bun);
  self.info = bun.info.bind(bun);
  self.debug = bun.debug.bind(bun);
  self.trace = (method, requestUrl, body, responseBody, responseStatus) => {
    bun.trace({
      method:         method,
      requestUrl:     requestUrl,
      body:           body,
      responseBody:   responseBody,
      responseStatus: responseStatus
    });
  };
  self.close = () => {
  };
};

const DEFAULT_ELASTICSEARCH_PORT = 9200;

const createEsClient = (hostConfig) => {
  const host = hostConfig.host || 'localhost';
  const port = hostConfig.port || DEFAULT_ELASTICSEARCH_PORT;

  let uri = `${host}:${port}`;
  if (!uri.startsWith('http://') && !uri.startsWith('https://')) {
    uri = `http://${uri}`;
  }

  /**
   *  NOTE: The below section should be removed and replaced with an async implementation.
   *        This is blocking the main thread while it waits for the result of the API check, meaning nothing else
   *        can run.
   */

  let apiVersion = null;
  try {
    const results = sr('GET', uri, {
      maxRetries: 5,
      retry:      true,
      timeout:    100
    });
    const version = JSON.parse(results.getBody('utf8')).version.number;
    apiVersion = `${semver.major(version)}.${semver.minor(version)}`;
  } catch (e) {
    config.log.debug(e);
  }

  if (!apiVersion) {
    throw new Error(`unable to connect to '${uri}' to get es version`);
  }

  return new elasticsearch.Client({
    host:               {host, port},
    apiVersion:         apiVersion,
    suggestCompression: true,
    log:                LogToBunyan,
    defer:              function () {
      let resolve = null;
      let reject  = null;

      const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
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