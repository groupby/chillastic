module.exports = {
  validation: {
    type:       'object',
    optional:   true,
    strict:     true,
    properties: {
      actions: {
        type:  'array',
        items: {
          type:       'object',
          strict:     true,
          properties: {
            namespace: {
              type:     'string',
              optional: true
            },
            id: {
              type: 'string'
            },
            arguments: {
              type:     'object',
              optional: true
            }
          }
        }
      },
      arguments: {
        type:     'object',
        optional: true
      }
    }
  },
  sanitization: {
    type:       'object',
    properties: {
      actions: {
        type:  'array',
        items: {
          type:       'object',
          properties: {
            namespace: {},
            id:        {},
            arguments: {
              type: 'object'
            }
          }
        }
      },
      arguments: {
        type: 'object'
      }
    }
  }
};