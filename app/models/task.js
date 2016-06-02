const _         = require('lodash');
const inspector = require('schema-inspector');

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
    spec:        {
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
              type:       'object',
              optional:   true,
              properties: {
                indices: {
                  type:       'object',
                  optional:   true,
                  properties: {
                    value: {
                      type:      'string',
                      minLength: 1
                    },
                    type:  {
                      type:    'string',
                      pattern: /^path$|^regex$/,
                      error:   `Must be 'path' or 'regex'`
                    }
                  }
                },
                types:   {
                  type:       'object',
                  optional:   true,
                  properties: {
                    value: {
                      type:      'string',
                      minLength: 1
                    },
                    type:  {
                      type:    'string',
                      pattern: /^path$|^regex$/,
                      error:   `Must be 'path' or 'regex'`
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    mutators:    {
      type:     'string',
      optional: true
    }
  }
};

const Task = function (params) {
  const self = this;

  inspector.sanitize(SCHEMA, params);
  const result = inspector.validate(SCHEMA, params);

  if (!result.valid) {
    throw new Error(result.format());
  }

  _.merge(self, params);

  return self;
};

Task.NAME_REGEX = /^[a-zA-Z][a-zA-Z0-9]{1,40}$/;

module.exports = Task;