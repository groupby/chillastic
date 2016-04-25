module.exports = {
  type:      'wrong',
  predicate: (index) => {
    return index.name === 'index_to_mutate';
  },
  mutate:    (index) => {
    index.name = 'new_index_name';
    return index;
  }
};