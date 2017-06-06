const _         = require('lodash');
const inspector = require('./inspector');
const Task      = require('./task');
const utils     = require('../../config/utils');

const VALIDATION_SCHEMA = {
  type:       'object',
  strict:     true,
  properties: {
    source: {
      $type: 'elasticsearch_v'
    },
    destination: {
      $type: 'elasticsearch_v'
    },
    transfer: {
      type:       'object',
      strict:     true,
      properties: {
        flushSize: {
          type:     'integer',
          optional: false,
          def:      Task.DEFAULT_FLUSH_SIZE
        },
        index: {
          type:      'string',
          optional:  true,
          minLength: 1
        },
        template: {
          type:      'string',
          optional:  true,
          minLength: 1
        },
        documents: {
          type:       'object',
          optional:   true,
          strict:     true,
          properties: {
            index: {
              type:      'string',
              minLength: 1
            },
            type: {
              type:      'string',
              minLength: 1
            }
          }
        }
      }
    },
    mutators: {
      $type:    'mutators_v',
      optional: true
    },
    count: {
      type: 'integer',
      gte:  0
    }
  }
};

const SANITIZATION_SCHEMA = {
  type:       'object',
  properties: {
    source: {
      $type: 'elasticsearch_s'
    },
    destination: {
      $type: 'elasticsearch_s'
    },
    transfer: {
      type:       'object',
      properties: {
        flushSize: {
          type:     'integer',
          optional: false,
          def:      Task.DEFAULT_FLUSH_SIZE
        },
        index: {
          minLength: 1
        },
        template: {
          minLength: 1
        },
        documents: {
          type:       'object',
          optional:   true,
          properties: {
            index: {
              minLength: 1
            },
            type: {
              minLength: 1
            }
          }
        }
      }
    },
    mutators: {
      $type:    'mutators_S',
      optional: true
    },
    count: {
      type: 'integer',
      gte:  0
    }
  }
};

const Subtask  = function (params) {
  const self = this;

  inspector.sanitize(SANITIZATION_SCHEMA, params);
  const result = inspector.validate(VALIDATION_SCHEMA, params);

  if (!result.valid) {
    throw new Error(result.format());
  }

  _.merge(self, params);

  const idSource = {};
  _.merge(idSource, params);
  delete idSource.count;

  self.getID = () => JSON.stringify(idSource);
  self.toString = () => JSON.stringify(params);

  return self;
};
Subtask.coerce = (subtask) => subtask instanceof Subtask ? subtask : new Subtask(subtask);

/**
 * Static factory for creating subtasks directly from the ID and count
 *
 * @param id
 * @param count
 * @returns {Subtask}
 */
Subtask.createFromID = (id, count) => {
  if (!utils.isNonZeroString(id)) {
    throw new Error('id must be stringified json');
  }

  const params = JSON.parse(id);
  params.count = count;

  return new Subtask(params);
};

module.exports = Subtask;