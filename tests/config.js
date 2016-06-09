/*eslint no-process-env: "off"*/
module.exports = require(`./config-${process.env['environment'] || 'local'}`);