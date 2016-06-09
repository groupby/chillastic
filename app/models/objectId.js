const _         = require('lodash');
const Promise   = require('bluebird');
const inspector = require('./inspector');

const SCHEMA = {
  type:       'object',
  properties: {
    namespace: {
      type:     'string',
      def:      'global',
      optional: false
    },
    id:        {
      type:     'string',
      optional: false
    }
  }
};

const ObjectId    = function (params) {
  const self       = this;
  const validateId = (id)=> _.isString(id) && ObjectId.ID_REGEX.test(id);

  inspector.sanitize(SCHEMA, params);
  const result = inspector.validate(SCHEMA, params);

  if (!result.valid) {
    throw new Error(result.format());
  }
  _.merge(self, params);

  self.validate = (name)=> new Promise((resolve, reject)=> {
    name = name || 'Id';

    if (!validateId(self.namespace)) {
      reject(new Error(`Namespace must be string of 1-40 alphanumeric characters, given '${self.namespace}'`));
    }
    if (!validateId(self.id)) {
      reject(new Error(`${name} must be string of 1-40 alphanumeric characters, given '${self.id}'`));
    }
    resolve(self);
  });
  return self;
};
ObjectId.ID_REGEX = /^[a-zA-Z][a-zA-Z0-9]{1,40}$/;
ObjectId.coerce   = (id)=> id instanceof ObjectId ? id : new ObjectId(id);

module.exports = ObjectId;
