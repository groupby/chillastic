/*eslint no-magic-numbers: "off"*/
/*eslint no-invalid-this: "off"*/
const _                 = require('lodash');
const expect            = require('chai').expect;
const Promise           = require('bluebird');
const TestConfig        = require('../config');
const Utils             = require('../utils');
const Manager           = require('../../app/services/manager');
const createEsClient    = require('../../config/elasticsearch');
const createRedisClient = require('../../config/redis');
const config            = require('../../config/index');

const log = config.log;

Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection((error) => log.error('Likely error: ', error.stack));

describe('job manager', function () {
  this.timeout(5000);

  let source  = null;
  let redis   = null;
  let manager = null;
  let utils   = null;

  before((done) => {
    source = createEsClient(TestConfig.elasticsearch.source);
    redis = createRedisClient(TestConfig.redis.host, TestConfig.redis.port);
    manager = new Manager(redis);
    utils = new Utils();

    utils.deleteAllTemplates(source)
      .finally(() => utils.deleteAllIndices(source))
      .finally(() => redis.flushdb())
      .finally(() => done());
  });

  after((done) => {
    redis.quit().finally(() => done());
  });

  afterEach((done) => {
    utils.deleteAllTemplates(source)
      .finally(() => utils.deleteAllIndices(source))
      .finally(() => redis.flushdb())
      .finally(() => done());
  });

  it('should not accept the same name twice', (done) => {
    const names = [
      'same',
      'same',
      'different'
    ];

    const getName = () => {
      return names.shift();
    };

    manager._setWorkerName(getName).then((name) => {
      expect(name).to.be.equals('same');
      return manager._setWorkerName(getName);
    }).then((name) => {
      expect(name).to.be.equals('different');
      done();
    }).catch(done);
  });

  it('should expire old names', function (done) {
    this.timeout(2000);
    const names = [
      'same',
      'same'
    ];

    manager._overrideNameTimeout(1);

    const getName = () => {
      return names.shift();
    };

    manager._setWorkerName(getName).delay(1100).then((name) => {
      expect(name).to.be.equals('same');
      return manager._setWorkerName(getName);
    }).then((name) => {
      expect(name).to.be.equals('same');
      done();
    }).catch(done);
  });

  it('should expire worker statuses', function (done) {
    this.timeout(2000);
    const names = [
      'same',
      'different'
    ];

    manager._overrideNameTimeout(1);

    const getName = () => {
      return names.shift();
    };

    manager._setWorkerName(getName).then((name) => {
      expect(name).to.be.equals('same');
      return manager._setWorkerName(getName);
    }).then((name) => {
      expect(name).to.be.equals('different');
      return manager.workerHeartbeat('same', 'running');
    }).then(() => {
      return manager.workerHeartbeat('different', 'broken');
    }).then(() => {
      return manager.getWorkersStatus();
    }).delay(500).then((status) => {
      expect(_.size(status)).to.be.equals(2);
      expect(status.same).to.be.equals('running');
      expect(status.different).to.be.equals('broken');
      return manager.workerHeartbeat('same', 'running');
    }).delay(600).then(() => {
      return manager.getWorkersStatus();
    }).then((status) => {
      expect(_.size(status)).to.be.equals(1);
      expect(status.same).to.be.equals('running');
      done();
    }).catch(done);
  });
});
