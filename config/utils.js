import path from 'path';
import _ from 'lodash';

var parsePath = (input)=> {
  if (path.isAbsolute(input)) {
    return input;
  } else {
    return path.join(path.resolve('.'), input);
  }
};

var isNonZeroString = (input) => {
  return _.isString(input) && input.length > 0;
};

export default {
  parsePath:       parsePath,
  isNonZeroString: isNonZeroString
};