const moment = require('moment');

const OLD_DATE_FORMAT = 'YYYY-MM-DD';
const OLD_DATE_REGEX  = /[0-9]{4}-[0-9]{2}-[0-9]{2}/;
const NEW_DATE_FORMAT = 'YYYY-MM';

module.exports = {
  type:      'data',
  predicate: function (doc) {
    return OLD_DATE_REGEX.test(doc._index);
  },
  mutate:    function (doc) {
    const date = moment(doc._index.match(OLD_DATE_REGEX), OLD_DATE_FORMAT);
    doc._index = doc._index.replace(OLD_DATE_REGEX, date.format(NEW_DATE_FORMAT));

    return doc;
  }
};