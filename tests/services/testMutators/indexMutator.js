module.exports = {
  type:      'index',
  predicate: (index) => {
    return index.name === 'index_to_mutate';
  },
  mutate:    (index) => {
    index.name = 'new_index_name';
    return index;
  }
};
