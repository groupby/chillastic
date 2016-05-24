const _ = require('lodash');

module.exports = {
  type:      'data',
  predicate: (doc)=> {
    if (_.has(doc._source, 'products') || _.has(doc._source, 'product')) {
      return true;
    } else {
      return false;
    }
  },
  mutate:    (doc)=> {
    if (_.has(doc._source, 'products')) {
      doc._source.products = _.map((product)=> {
        if (_.isInteger(product.price)) {
          product.price = product.price.toFixed(2);
        }

        return product;
      });
    } else {
      if (_.isInteger(doc._source.product.price)) {
        doc._source.product.price = doc._source.product.price.toFixed(2);
      }
    }

    return doc;
  }
};
