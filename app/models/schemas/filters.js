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
    properties: {
      actions: {
        items: {
          properties: {
            namespace: {},
            id:        {},
            arguments: {}
          }
        }
      },
      arguments: {}
    }
  }
};