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
    optional:   true,
    properties: {
      actions: {
        type:  'array',
        items: {
          type:       'object',
          properties: {
            namespace: {
              optional: true
            },
            id:        {},
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
  }
};