const _ = require('lodash');

module.exports = {
  type:      'index',
  predicate: (index)=> index.name === 'index_to_mutate',
  mutate:    (index)=> _.assign(index, {name: 'new_index_name'})
};
