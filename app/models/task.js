const _         = require('lodash');
const inspector = require('./inspector');
const ObjectId  = require('./objectId');

const Task = function (params) {
  const self = this;

  inspector.sanitize(SANITIZATION_SCHEMA, params);
  const result = inspector.validate(VALIDATION_SCHEMA, params);

  if (!result.valid) {
    throw new Error(result.format());
  }

  _.merge(self, params);

  return self;
};

Task.validateId = (id) => new ObjectId({id: id}).validate('taskId');
Task.coerce = (task) => task instanceof Task ? task : new Task(task);
Task.errorKey = (taskId) => `${taskId}_error`;
Task.totalKey = (taskId) => `${taskId}_total`;
Task.progressKey = (taskId) => `${taskId}_progress`;
Task.completedKey = (taskId) => `${taskId}_completed`;
Task.backlogQueueKey = (taskId) => `${taskId}_backlog_queue`;
Task.backlogHSetKey = (taskId) => `${taskId}_backlog_hset`;

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
        indices: {
          type:       'object',
          optional:   true,
          properties: {
            name:      {},
            templates: {}
          }
        },
        documents: {
          type:       'object',
          optional:   true,
          properties: {
            fromIndices: {
              minLength: 1
            },
            filters: {
              $type:    'filters_s',
              optional: true
            }
          }
        }
      }
    },
    mutators: {
      $type:    'mutators_s',
      optional: true
    }
  }
};

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
        indices: {
          type:       'object',
          strict:     true,
          optional:   true,
          properties: {
            name: {
              type:     'string',
              optional: true
            },
            templates: {
              type:     'string',
              optional: true
            }
          }
        },
        documents: {
          type:       'object',
          strict:     true,
          optional:   true,
          properties: {
            fromIndices: {
              type:      'string',
              minLength: 1
            },
            filters: {
              $type:    'filters_v',
              optional: true
            }
          }
        }
      }
    },
    mutators: {
      $type:    'mutators_v',
      optional: true
    }
  }
};

module.exports = Task;
