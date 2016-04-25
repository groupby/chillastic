module.exports = {
  type:      'template',
  predicate: (template) => {
    return template.template === 'template_this*';
  },
  mutate:    (template) => {
    template.template = 'template_that*';
    return template;
  }
};