/*eslint no-magic-numbers: "off"*/
/*eslint no-invalid-this: "off"*/
const _                 = require('lodash');
const expect            = require('chai').expect;
const Promise           = require('bluebird');
const TestConfig        = require('../config');
const Utils             = require('../utils');
const Manager           = require('../../app/services/manager');
const Subtasks          = require('../../app/services/subtasks');
const Tasks             = require('../../app/services/tasks');
const Worker            = require('../../app/services/worker');
const createEsClient    = require('../../config/elasticsearch.js');
const createRedisClient = require('../../config/redis');
const config            = require('../../config/index');

const log   = config.log;
const utils = new Utils();

Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection((error) => log.error('Likely error: ', error.stack));

describe('worker', function () {
  this.timeout(10000);

  const TASK1_NAME = 'task1';
  let source       = null;
  let dest         = null;
  let redis        = null;
  let manager      = null;
  let tasks        = null;
  let subtasks     = null;
  let worker       = null;

  before((done) => {
    Worker.__overrideCheckInterval(100);

    source = createEsClient(TestConfig.elasticsearch.source);
    dest = createEsClient(TestConfig.elasticsearch.destination);
    redis = createRedisClient(TestConfig.redis.host, TestConfig.redis.port);
    manager = new Manager(redis);
    tasks = new Tasks(redis);
    subtasks = new Subtasks(redis);
    worker = new Worker(redis);

    utils.deleteAllTemplates(source)
      .finally(() => utils.deleteAllTemplates(dest))
      .finally(() => utils.deleteAllIndices(source))
      .finally(() => utils.deleteAllIndices(dest))
      .finally(() => redis.flushdb())
      .finally(() => done());
  });

  it('should perform transfers queued by manager', (done) => {
    const taskParams = {
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      transfer:    {
        documents: {
          fromIndices: '*'
        }
      }
    };

    const indexConfigs = [
      {index: 'first', type: 'type1'},
      {index: 'second', type: 'mytype1'}
    ];

    const data = [];
    _.times(10, (n) => {
      data.push({index: {_index: indexConfigs[0].index, _type: indexConfigs[0].type}});
      data.push({something: `data${n}`});
    });

    _.times(5, (n) => {
      data.push({index: {_index: indexConfigs[1].index, _type: indexConfigs[1].type}});
      data.push({something: `data${n}`});
    });

    let totalTransferred  = 0;
    const progressUpdates = (taskName, subtask, update) => totalTransferred += update.tick;

    const completedSubtasks = [];
    const completeCallback  = (taskName, subtask) => {
      expect(taskName).to.be.equals(TASK1_NAME);
      completedSubtasks.push(subtask);

      if (completedSubtasks.length >= 2) {
        expect(totalTransferred).to.be.equals(15);
        manager.setRunning(false);
        worker.killStopped();
        done();
      }
    };

    worker.setUpdateCallback(progressUpdates);
    worker.setCompletedCallback(completeCallback);

    source.indices.create({index: 'first'})
      .then(() => source.indices.create({index: 'second'}))
      .then(() => source.bulk({body: data}))
      .then((results) => {
        if (results.errors) {
          log.error('errors', JSON.stringify(results, null, 2));
          done('fail');
          return Promise.reject(`errors: ${results.errors}`);
        } else {
          return source.indices.refresh();
        }
      })
      .then(() => utils.deleteAllIndices(dest))
      .then(() => dest.search())
      .then((results) => expect(results.hits.total).to.be.equals(0))
      .then(() => dest.indices.create({index: 'first'}))
      .then(() => dest.indices.create({index: 'second'}))
      .then(() => tasks.add(TASK1_NAME, taskParams))
      .then(() => subtasks.getBacklog(TASK1_NAME))
      .then((backlogJobs) => {
        expect(backlogJobs.length).to.be.equals(2);

        let target = _.find(backlogJobs, {
          transfer: {documents: {index: 'first', type: 'type1'}}
        });
        expect(target.count).to.be.equals(10);

        target = _.find(backlogJobs, {
          transfer: {documents: {index: 'second', type: 'mytype1'}}
        });
        expect(target.count).to.be.equals(5);
      })
      .then(() => manager.setRunning(true));
  });
});