const compression    = require('compression');
const bodyParser     = require('body-parser');
const methodOverride = require('method-override');
const cookieParser   = require('cookie-parser');
const errorHandler   = require('errorhandler');
const cors           = require('cors');
const expressLogger  = require('express-bunyan-logger');
const config         = require('./index');
const PrettyStream   = require('bunyan-prettystream');
const prettyStdOut   = new PrettyStream({mode: 'dev'});
prettyStdOut.pipe(process.stdout);

module.exports = function (app) {
  const env = app.get('env');

  const defaultContentTypeMiddleware = (req, res, next) => {
    req.headers['content-type'] = 'application/json';
    next();
  };

  app.use(defaultContentTypeMiddleware);

  app.use(compression());
  app.use(bodyParser.urlencoded({extended: false}));
  app.use(bodyParser.json());
  app.use(methodOverride());
  app.use(cookieParser());
  app.use(cors());
  app.use(expressLogger({
    name:     `${config.FRAMEWORK_NAME}-express`,
    format:   ":status-code - :method :url - response-time: :response-time",
    streams:  [
      {
        level:  'info',
        stream: prettyStdOut
      }
    ],
    excludes: ['*'],
    levelFn:  (status, err, meta)=> {
      if (meta["response-time"] > 30000) {
        return "fatal";
      } else if (meta["status-code"] >= 500) {
        return "error";
      } else if (meta["status-code"] >= 400) {
        return "warn";
      } else {
        return "info";
      }
    }
  }));

  if ('development' === env || 'test' === env) {
    app.use(errorHandler()); // Error handler - has to be last
  }
};