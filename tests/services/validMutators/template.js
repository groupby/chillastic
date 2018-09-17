const _ = require('lodash');

module.exports = {
  type:      'template',
  predicate: (template) => template.index_patterns[0] === 'template_this*',
  mutate:    (template) => _.assign(template, {index_patterns: ['template_that*']})
};