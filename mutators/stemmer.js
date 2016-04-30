import _ from 'lodash';

module.exports = {
  type:      'template',
  predicate: function (template) {
    return _.has(template, 'mappings.search.properties.search.properties.searchTerm');
  },
  mutate:    function (template) {
    template.mappings.search.properties.search.properties.searchTerm.fields.lang_en = {
      type:     'string',
      analyzer: 'english'
    };

    return template;
  }
};