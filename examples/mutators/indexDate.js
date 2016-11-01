// The libraries for 'moment' and 'lodash' are available inside the mutator definition
const moment = require('moment');

const OLD_DATE_FORMAT = 'YYYY-MM-DD';
const OLD_DATE_REGEX  = /[0-9]{4}-[0-9]{2}-[0-9]{2}/;
const NEW_DATE_FORMAT = 'YYYY-MM';

module.exports = {
  /**
   * Type of mutator
   */
  type:      'data',
  /**
   * The predicate function is called for every target document
   * @param doc - The document to be checked against the predicate
   * @param arguments - The task-specific arguments object
   * @returns {boolean}
   */
  predicate: function (doc, arguments) {
    return OLD_DATE_REGEX.test(doc._index);
  },
  /**
   * The mutate function is only called on documents that satisfy the predicate
   * @param doc - The document that satisfied the predicate
   * @param arguments - The task-specific arguments object
   * @returns {*}
   */
  mutate: function (doc, arguments) {
    const date = moment(doc._index.match(OLD_DATE_REGEX), OLD_DATE_FORMAT);
    doc._index = doc._index.replace(OLD_DATE_REGEX, date.format(NEW_DATE_FORMAT));

    return doc;
  }
};