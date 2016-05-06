module.exports = {
  type:      'index',
  predicate: function (mapping) {
    return mapping.name === 'config';
  },
  mutate:    function (index) {
    // Remove specific types
    delete index.mappings.order;
    delete index.mappings.search;

    return index;
  }
};