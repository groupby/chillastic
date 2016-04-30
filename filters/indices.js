import moment from 'moment';

var OLD_DATE_FORMAT = 'YYYY-MM-DD';
var OLD_DATE_REGEX  = /[0-9]{4}-[0-9]{2}-[0-9]{2}$/;

module.exports = (index)=>{
  if (OLD_DATE_REGEX.test(index.name)) {

    let date = moment(index.name.match(OLD_DATE_REGEX), OLD_DATE_FORMAT);
    let endDate   = moment('2016-04-29', OLD_DATE_FORMAT);
    let startDate = moment(0);

    if (date.isSameOrBefore(endDate) && date.isAfter(startDate)) {
      return true;
    } else {
      return false;
    }
  } else {
    return true;
  }
};