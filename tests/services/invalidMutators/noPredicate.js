module.exports = {
  type:   'index',
  mutate: (index) => {
    index.name = 'new_index_name';
    return index;
  }
};