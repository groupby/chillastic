const _         = require('lodash');
const inspector = require('schema-inspector');
const utils     = require('../../config/utils');

const SCHEMA = {
  type:       'object',
  properties: {
    source:      {
      type:       'object',
      properties: {
        host:       {
          type:      'string',
          minLength: 3
        },
        apiVersion: {
          type:      'string',
          minLength: 3
        }
      }
    },
    destination: {
      type:       'object',
      properties: {
        host:       {
          type:      'string',
          minLength: 3
        },
        apiVersion: {
          type:      'string',
          minLength: 3
        }
      }
    },
    transfer:    {
      type:       'object',
      properties: {
        documents: {
          type:       'object',
          optional:   true,
          properties: {
            index: {
              type:      'string',
              minLength: 1
            },
            type:  {
              type:      'string',
              minLength: 1
            }
          }
        },
        index:     {
          type:      'string',
          optional:  true,
          minLength: 1
        },
        template:  {
          type:      'string',
          optional:  true,
          minLength: 1
        }
      }
    },
    mutators:    {
      type:     'string',
      optional: true
    },
    count:       {
      type: 'integer',
      gte:  0
    }
  }
};

const Subtask = function (params) {
  const self = this;

  inspector.sanitize(SCHEMA, params);
  const result = inspector.validate(SCHEMA, params);

  if (!result.valid) {
    throw new Error(result.format());
  }

  _.merge(self, params);

  const idSource = {};
  _.merge(idSource, params);
  delete idSource.count;

  self.getID = ()=> {
    return JSON.stringify(idSource);
  };

  self.toString = ()=> {
    return JSON.stringify(params);
  };

  return self;
};

/**
 * Static factory for creating subtasks directly from the ID and count
 *
 * @param id
 * @param count
 * @returns {Subtask}
 */
Subtask.createFromID = (id, count)=> {

  if (!utils.isNonZeroString(id)) {
    throw new Error('id must be stringified json');
  }

  let params   = JSON.parse(id);
  params.count = count;

  return new Subtask(params);
};

module.exports = Subtask;