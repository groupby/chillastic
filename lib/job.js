import _ from 'lodash';

import config from '../config';
var log = config.log;

var isNonZeroString = (input) => {
  return _.isString(input) && input.length > 0;
};

/**
 * Job constructor
 *
 * Requires index, type, and count.
 *
 * @param params
 * @constructor
 */
var Job = function (params) {
  var self = this;

  if (!isNonZeroString(params.index)) {
    throw new Error('index must be string with length');
  }

  if (!isNonZeroString(params.type)) {
    throw new Error('type must be string with length');
  }

  params.count = parseInt(params.count);

  if (!_.isNumber(params.count) || _.isNaN(params.count) || params.count < 0) {
    throw new Error('count must be number gte 0');
  }

  self.index = params.index;
  self.type  = params.type;
  self.count = params.count;

  self.getID = ()=> {
    return JSON.stringify({
      index: self.index,
      type:  self.type
    });
  };

  self.toString = ()=> {
    return JSON.stringify({
      index: self.index,
      type:  self.type,
      count: self.count
    });
  }
};

/**
 * Static factory for creating jobs directly from the ID and count
 *
 * @param id
 * @param count
 * @returns {Job}
 */
Job.createFromID = (id, count)=> {

  if (!isNonZeroString(id)) {
    throw new Error('id must be stringified json');
  }

  let params   = JSON.parse(id);
  params.count = count;

  return new Job(params);
};

export default Job;