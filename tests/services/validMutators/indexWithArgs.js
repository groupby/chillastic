const _ = require('lodash');

module.exports = {
  type:      'index',
  predicate: (index, args) => index.name === args.target,
  mutate:    (index, args) => _.assign(index, {name: args.name})
};
