module.exports = {
  type:      'index',
  predicate: (index, args) => {
    return index.name === args.target;
  },
  mutate:    (index, args) => {
    index.name = args.name;
    return index;
  }
};
