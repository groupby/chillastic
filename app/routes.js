const _          = require('lodash');
const express    = require('express');
const router     = express.Router();
const cors       = require('cors');
const HttpStatus = require('http-status');

const api    = require('./api');
const config = require('../config');

module.exports = (app) => {
  const log = config.log;

  // Check for API key regardless of the route
  app.use('*', router.use((req, res, next) => {
    if (_.isString(config.apiKey)) {
      const apiKey = req.headers.api_key || req.headers.API_KEY;

      if (!_.isString(apiKey)) {
        res.status(HttpStatus.BAD_REQUEST);
        res.send({error: 'api key required'});
      } else {
        log.info('api key provided');

        if (apiKey !== config.apiKey) {
          res.status(HttpStatus.UNAUTHORIZED);
          log.error('unknown api key');
          res.send({error: 'unknown api key'});
        } else {
          next();
        }
      }
    } else {
      next();
    }
  }));

  // Enable CORS pre-flight for all routes
  app.options('*', cors());

  // All valid routes handled here
  app.use(api());

  // Everything else is a 404
  app.use((req, res) => {
    res.status(HttpStatus.NOT_FOUND);
    res.send({error: 'Not found'});
  });
};