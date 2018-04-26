/*eslint no-console: "off" */
const checker = require('gb-license-check');

const PACKAGE_WHITELIST = {
  'commander':      ['^2.3.0'], // MIT license https://github.com/tj/commander.js/blob/master/LICENSE
  'flexbuffer':     ['^0.0.6'], // Has MIT license https://github.com/mercadolibre/flexbuffer-node/blob/master/LICENSE
  'log-driver':     ['^1.2.5'], // Has ISC license https://github.com/cainus/logdriver/blob/master/LICENSE
  'tweetnacl':      ['^0.14.3'], // Has unlimited license https://github.com/dchest/tweetnacl-js/blob/master/COPYING.txt
  'callsite':       ['1.0.0'], // Has MIT license at https://github.com/tj/callsite/blob/master/LICENSE
  'amdefine':       ['^1.0.1'], // Has MIT license at https://github.com/jrburke/amdefine
  'atob':           ['^2.1.0'], // Has MIT license at git://git.coolaj86.com/coolaj86/atob.js
  'inherits':       ['^1.0.2'], // Has MIT license at https://github.com/isaacs/inherits
  'path-is-inside': ['^1.0.2'] // Has MIT license at https://github.com/domenic/path-is-inside
};

checker.run(PACKAGE_WHITELIST, (err) => {
  if (err) {
    console.error('ERROR: Unknown licenses found');
    process.exit(1);
  }

  console.log('License check successful');
});