import elasticsearch from 'elasticsearch';
import Promise from 'bluebird';
import bunyan from 'bunyan';
import PrettyStream from 'bunyan-prettystream';
var prettyStdOut = new PrettyStream({mode: 'dev'});
prettyStdOut.pipe(process.stdout);

function LogToBunyan(host) {
  return function () {
    var bun = bunyan.createLogger({
      name:   'es-' + host,
      type:   'raw',
      level:  'warn',
      stream: prettyStdOut
    });

    this.error   = bun.error.bind(bun);
    this.warning = bun.warn.bind(bun);
    this.info    = bun.info.bind(bun);
    this.debug   = bun.debug.bind(bun);
    this.trace   = function (method, requestUrl, body, responseBody, responseStatus) {
      bun.trace({
        method:         method,
        requestUrl:     requestUrl,
        body:           body,
        responseBody:   responseBody,
        responseStatus: responseStatus
      });
    };
    this.close   = function () { /* bunyan's loggers do not need to be closed */ };
  }
}

var create = (host, version) => {
  return new elasticsearch.Client({
    host:       host,
    apiVersion: version,
    log:        LogToBunyan(host),
    defer:      function () {
      return Promise.defer();
    }
  });
};

export default create;