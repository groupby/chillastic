const requireFromString = require('require-from-string');

const Compiler = function () {
  const self = this;

  self.compile = function (src) {
    try {
      return requireFromString(src);
    } catch (e) {
      throw new Error(`Unable to load external module due to '${e.name} - ${e.message}'`);
    }
  };
};

module.exports = Compiler;