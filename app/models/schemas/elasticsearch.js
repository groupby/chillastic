module.exports = {
  sanitization: {
    type:       'object',
    properties: {
      host: {},
      port: {
        type:     'integer',
        optional: false,
        def:      9200
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
      }
    }
  }
};
