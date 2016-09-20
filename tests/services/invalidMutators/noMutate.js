module.exports = {
  type:      'index',
  predicate: (index) => {
    return index.name === 'index_to_mutate';
  }
};