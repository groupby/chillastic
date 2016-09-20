module.exports = {
  type:      'data',
  predicate: (doc, args) => doc._source.field === args.match,
  mutate:    () => null
};
