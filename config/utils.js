const path = require('path');
const _ = require('lodash');

const parsePath = (input)=> {
  if (path.isAbsolute(input)) {
    return input;
  } else {
    return path.join(path.resolve('.'), input);
  }
};

const isNonZeroString = (input) => {
  return _.isString(input) && input.length > 0;
};

module.exports = {
  parsePath:       parsePath,
  isNonZeroString: isNonZeroString
};