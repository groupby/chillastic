import moment from 'moment';

var OLD_DATE_FORMAT = 'YYYY-MM-DD';
var OLD_DATE_REGEX  = /[0-9]{4}-[0-9]{2}-[0-9]{2}/;
var NEW_DATE_FORMAT = 'YYYY-MM';

module.exports = {
  type:      'data',
  predicate: (doc) => {
    return OLD_DATE_REGEX.test(doc._index)
  },
  mutate:    (doc) => {
    var date   = moment(doc._index.match(OLD_DATE_REGEX), OLD_DATE_FORMAT);
    doc._index = doc._index.replace(OLD_DATE_REGEX, date.format(NEW_DATE_FORMAT));
    return doc;
  }
};