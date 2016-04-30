import _ from 'lodash';

module.exports = {
  type:      'template',
  predicate: function () {
    return true;
  },
  mutate:    function (template) {
    template.settings['index.number_of_shards'] = 3;
    template.settings['index.number_of_replicas'] = 1;
    return template;
  }
};