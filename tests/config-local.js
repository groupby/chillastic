module.exports = {
  'elasticsearch': {
    'source': {
      'host': 'es',
      'port': 9200
    },
    'destination': {
      'host': 'es-new',
      'port': 9200
    }
  },
  'redis': {
    'host': 'redis',
    'port': 6379
  }
};