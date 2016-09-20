const _         = require('lodash');
const inspector = require('./inspector');

const ISO_86001_REGEX = /(\d{4})-(0[1-9]|1[0-2]|[1-9])-(\3([12]\d|0[1-9]|3[01])|[1-9])[tT\s]([01]\d|2[0-3])\:(([0-5]\d)|\d)\:(([0-5]\d)|\d)([\.,]\d+)?([zZ]|([\+-])([01]\d|2[0-3]|\d):(([0-5]\d)|\d))$/;

const SCHEMA = {
  type:       'object',
  properties: {
    tick: {
      type: 'integer',
      gte:  0
    },
    transferred: {
      type: 'integer',
      gte:  0
    },
    total: {
      type: 'integer',
      gte:  0
    },
    worker: {
      type:     'string',
      optional: true
    },
    lastModified: {
      type:     'string',
      optional: true,
      pattern:  ISO_86001_REGEX,
      error:    'if provided, must be an ISO 86001 date'
    }
  }
};

const Progress  = function (params) {
  const self = this;

  inspector.sanitize(SCHEMA, params);
  const result = inspector.validate(SCHEMA, params);

  if (!result.valid) {
    throw new Error(result.format());
  }

  _.merge(self, params);

  return self;
};
Progress.coerce = (progress) => progress instanceof Progress ? progress : new Progress(progress);

module.exports = Progress;