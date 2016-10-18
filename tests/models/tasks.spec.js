/*eslint no-magic-numbers: "off"*/
/*eslint no-invalid-this: "off"*/
const _          = require('lodash');
const expect     = require('chai').expect;
const Promise    = require('bluebird');
const TestConfig = require('../config');
const Task       = require('../../app/models/task');
const config     = require('../../config/index');

const log = config.log;

Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection((error) => log.error('Likely error: ', error.stack));

const TASK_NAME = 'testTask';

describe('task model', function () {
  it('should reject task with malformed mutator', () => {
    const taskParams = {
      source: {
        host: 'something',
        port: 9200
      },
      destination: {
        host: 'something',
        port: 9200
      },
      transfer: {
        documents: {
          fromIndices: '*'
        }
      },
      mutators: {
        actions: {
          namespace: 'someNamespace',
          id:        'someId'
        }
      }
    };

    expect(() => new Task(taskParams)).to.throw(/actions/);
  });
});