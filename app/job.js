const _ = require('lodash');

const utils = require('../config/utils');

/**
 * Job constructor
 *
 * Requires index, type, and count.
 *
 * @param params
 * @constructor
 */
const Job = function (params) {
  const self = this;

  if (!utils.isNonZeroString(params.index)) {
    throw new Error('index must be string with length');
  }

  if (!utils.isNonZeroString(params.type)) {
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

  if (!utils.isNonZeroString(id)) {
    throw new Error('id must be stringified json');
  }

  let params   = JSON.parse(id);
  params.count = count;

  return new Job(params);
};

module.exports = Job;