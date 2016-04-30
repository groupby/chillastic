import _ from 'lodash';

module.exports = {
  type:      'index',
  predicate: function (mapping) {
    return mapping.name === 'groupbyinc_config';
  },
  mutate:    function (index) {
    // Remove pointless types (contaminated from other template?)
    delete index.mappings.order;
    delete index.mappings.search;
    delete index.mappings.addToBasket;
    delete index.mappings.viewProduct;

    // Remove 'enabled' fields, break in 2.2
    // Remove 'store' fields, just unnecessary
    _.has(index.mappings, 'recommendation.properties.createdTime.enabled') && delete index.mappings.recommendation.properties.createdTime.enabled;
    _.has(index.mappings, 'recommendation.properties.createdTime.store') && delete index.mappings.recommendation.properties.createdTime.store;
    _.has(index.mappings, 'recommendation.properties.name') && delete index.mappings.recommendation.properties.name;
    _.has(index.mappings, 'user.properties.createdTime.enabled') && delete index.mappings.user.properties.createdTime.enabled;
    _.has(index.mappings, 'user.properties.createdTime.store') && delete index.mappings.user.properties.createdTime.store;
    _.has(index.mappings, 'user.properties.name') && delete index.mappings.user.properties.name;
    _.has(index.mappings, 'customer.properties.createdTime.enabled') && delete index.mappings.customer.properties.createdTime.enabled;
    _.has(index.mappings, 'customer.properties.createdTime.store') && delete index.mappings.customer.properties.createdTime.store;
    _.has(index.mappings, 'customer.properties.name') && delete index.mappings.customer.properties.name;
    _.has(index.mappings, 'project.properties.createdTime.enabled') && delete index.mappings.project.properties.createdTime.enabled;
    _.has(index.mappings, 'project.properties.createdTime.store') && delete index.mappings.project.properties.createdTime.store;

    return index;
  }
};