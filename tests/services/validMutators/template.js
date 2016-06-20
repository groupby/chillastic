const _ = require('lodash');

module.exports = {
  type:      'template',
  predicate: (template) => template.template === 'template_this*',
  mutate:    (template) => _.assign(template, {template: 'template_that*'})
};