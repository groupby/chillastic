module.exports = {
  sanitization: {
    properties: {
      host: {},
      port: {
        type:     'integer',
        optional: false,
        def:      9200
      },
      path: {
        type:     'string',
        optional: true,
        def:      '/'
      }
    }
  },
  validation: {
    strict:     true,
    type:       'object',
    properties: {
      host: {
        type: 'string'
      },
      port: {
        type:     'integer',
        optional: false
      },
      path: {
        type: 'string'
      }
    }
  }
};
