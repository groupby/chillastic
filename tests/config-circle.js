module.exports = {
  'elasticsearch': {
    'source': {
      'host': 'source_es',
      'port': 9200
    },
    'destination': {
      'host': 'dest_es',
      'port': 9200
    }
  },
  'redis': {
    'host': 'redis',
    'port': 6379
  }
};