const _         = require('lodash');
const inspector = require('./inspector');
const ObjectId  = require('./objectId');

const SCHEMA = {
  type:       'object',
  properties: {
    source:      {
      $type: 'elasticsearch'
    },
    destination: {
      $type: 'elasticsearch'
    },
    transfer:    {
      type:       'object',
      properties: {
        indices:   {
          type:       'object',
          optional:   true,
          properties: {
            name:      {
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
          optional:   true,
          properties: {
            fromIndices: {
              type:      'string',
              minLength: 1
            },
            filters:     {
              $type:    'filters',
              optional: true
            }
          }
        }
      }
    },
    mutators:    {
      $type:    'mutators',
      optional: true
    }
  }
};

const Task           = function (params) {
  const self = this;

  inspector.sanitize(SCHEMA, params);
  const result = inspector.validate(SCHEMA, params);

  if (!result.valid) {
    throw new Error(result.format());
  }

  _.merge(self, params);

  return self;
};
Task.validateId      = (id)=> new ObjectId({id: id}).validate('taskId');
Task.coerce          = (task)=> task instanceof Task ? task : new Task(task);
Task.errorKey        = (taskId)=> `${taskId}_error`;
Task.progressKey     = (taskId)=> `${taskId}_progress`;
Task.completedKey    = (taskId)=> `${taskId}_completed`;
Task.backlogQueueKey = (taskId)=> `${taskId}_backlog_queue`;
Task.backlogHSetKey  = (taskId)=> `${taskId}_backlog_hset`;

module.exports = Task;
