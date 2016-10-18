const _             = require('lodash');
const inspector     = require('schema-inspector');
const elasticsearch = require('./schemas/elasticsearch');
const mutators      = require('./schemas/mutators');
const filters       = require('./schemas/filters');

const CUSTOM_TYPES = {
  elasticsearch_s: elasticsearch.sanitization,
  elasticsearch_v: elasticsearch.validation,
  mutators_s:      mutators.sanitization,
  mutators_v:      mutators.validation,
  filters_s:       filters.sanitization,
  filters_v:       filters.validation
};

const getCustomTypeFromSchema = (schema) => _.get(CUSTOM_TYPES, schema.$type, {type: schema.$type});

inspector.Sanitization.extend({
  type: function (schema, candidate) {
    inspector.sanitize(getCustomTypeFromSchema(schema), candidate);
  }
});

inspector.Validation.extend({
  type: function (schema, candidate) {
    const result = inspector.validate(getCustomTypeFromSchema(schema), candidate);
    if (!result.valid) {
      return this.report(result.format());
    }
  }
});

module.exports = inspector;
