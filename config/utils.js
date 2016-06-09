const _          = require('lodash');
const path       = require('path');
const HttpStatus = require('http-status');

const parsePath = (input)=> {
  if (path.isAbsolute(input)) {
    return input;
  } else {
    return path.join(path.resolve('.'), input);
  }
};

const isNonZeroString = (input) => {
  return _.isString(input) && input.length > 0;
};

const processError = function (error, res) {
  const log = require('../config').log;

  let message = '';
  let code    = HttpStatus.BAD_REQUEST;
  if (_.isUndefined(error)) {
    log.error('undefined error caught', error);
    message = 'error triggered, but error is undefined';
  } else if (_.isString(error.message) && error.message.indexOf('does not exist') !== -1) {
    message = error;
    code    = HttpStatus.NOT_FOUND;
  } else if (_.isString(error)) {
    log.warn('error', error);
    message = error;
  } else if (_.isString(error.message)) {
    log.warn('error', error);
    message = error.message;
  } else {
    log.error('error caught, but of unknown format');
    message = 'error caught, but of unknown format';
  }

  res.status(code).json({error: message});
};

module.exports = {
  parsePath:       parsePath,
  isNonZeroString: isNonZeroString,
  processError:    processError
};