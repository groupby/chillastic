const _         = require('lodash');
const inspector = require('schema-inspector');

const CUSTOM_TYPES = {
  elasticsearch: require('./schemas/elasticsearch'),
  mutators:      require('./schemas/mutators'),
  filters:       require('./schemas/filters')
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
