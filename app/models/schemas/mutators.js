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
    optional:   true,
    properties: {
      actions: {
        items: {
          properties: {
            namespace: {
              optional: true
            },
            id:        {},
            arguments: {
              optional: true
            }
          }
        }
      },
      arguments: {
        optional: true
      }
    }
  }
};