const moment = require('moment');

const OLD_DATE_FORMAT = 'YYYY-MM-DD';
const OLD_DATE_REGEX  = /[0-9]{4}-[0-9]{2}-[0-9]{2}$/;

module.exports = (a, b) => {
  if (OLD_DATE_REGEX.test(a) && OLD_DATE_REGEX.test(b)) {
    const aDate = moment(a.match(OLD_DATE_REGEX), OLD_DATE_FORMAT);
    const bDate = moment(b.match(OLD_DATE_REGEX), OLD_DATE_FORMAT);

    // Sort descending date
    const diff = bDate.valueOf() - aDate.valueOf();

    // Sort alphabetically if date is identical
    return (diff === 0) ? a.localeCompare(b) : diff;

  } else if (OLD_DATE_REGEX.test(a)) {
    return 1;
  } else if (OLD_DATE_REGEX.test(b)) {
    return -1;
  } else {
    return a.localeCompare(b);
  }
};