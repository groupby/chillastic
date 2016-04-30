import _ from 'lodash';

module.exports = {
  type:      'template',
  predicate: function (template) {
    return _.has(template, 'mappings.viewProduct') ||
      _.has(template, 'mappings.order') ||
      _.has(template, 'mappings.addToBasket');
  },
  mutate:    function (template) {
    let priceType = {
      type: 'double'
    };

    template.mappings.viewProduct.properties.product.properties.price = priceType;
    template.mappings.order.properties.products.properties.price = priceType;
    template.mappings.addToBasket.properties.product.properties.price = priceType;
    return template;
  }
};