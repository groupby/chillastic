/*eslint no-magic-numbers: "off"*/
/*eslint no-invalid-this: "off"*/
const _                 = require('lodash');
const expect            = require('chai').expect;
const Promise           = require('bluebird');
const TestConfig        = require('../config');
const Utils             = require('../utils');
const Filters           = require('../../app/services/filters');
const ObjectId          = require('../../app/models/objectId');
const createRedisClient = require('../../config/redis');
const config            = require('../../config/index');

const log = config.log;

Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection((error) => log.error('Likely error: ', error.stack));

const ns                = 'namespace';
const id                = 'id';
const topLevelArguments = {
  first:  'this thing',
  second: 'other thing'
};
const specificArguments = {
  second: 'second thing',
  third:  'last thing'
};
const validateFilter    = (filter) => expect(_.isFunction(filter.predicate)).is.true;

describe('filters service', function () {
  this.timeout(5000);

  let filters  = null;
  let redis    = null;
  let objectId = null;
  let utils    = null;

  before((done) => {
    redis = createRedisClient(TestConfig.redis.host, TestConfig.redis.port);
    filters = new Filters(redis);
    utils = new Utils();
    done();
  });

  after((done) => {
    redis.flushdb().finally(() => done());
  });

  beforeEach((done) => {
    objectId = new ObjectId({namespace: ns, id});
    redis.flushdb().finally(() => done());
  });

  it('invalid id', (done) => {
    objectId.id = '~badId';
    filters.add(objectId, 'dummySrc')
        .then(() => done('fail'))
        .catch((e) => expect(e.message).equals("Id must be string of 1-40 alphanumeric characters, given '~badId'"))
        .then(() => done());
  }
  );

  it('invalid src type', (done) => {
    filters.add(objectId, {})
    .then(() => done('fail'))
    .catch((e) => expect(e.message).equals('filterSrc must be string'))
    .then(() => done());
  });

  it('invalid javascript', (done) => {
    filters.add(objectId, utils.loadFile(`${__dirname}/invalidFilters/notAJsFile`))
    .then(() => done('fail'))
    .catch((e) => expect(e.message).equals("Unable to load external module due to 'SyntaxError - Unexpected identifier'"))
    .then(() => done());
  });

  it('filter missing type', (done) => {
    filters.add(objectId, utils.loadFile(`${__dirname}/invalidFilters/noType.js`))
    .then(() => done('fail'))
    .catch((e) => expect(e.message).equals('Filter type string not provided'))
    .then(() => done());
  });

  it('filter invalid type', (done) => {
    filters.add(objectId, utils.loadFile(`${__dirname}/invalidFilters/invalidType.js`))
    .then(() => done('fail'))
    .catch((e) => expect(e.message).equals(`Filter type \'wrong\' not one of: [${Filters.TYPES}]`))
    .then(() => done());
  });

  it('filter missing predicate', (done) => {
    filters.add(objectId, utils.loadFile(`${__dirname}/invalidFilters/noPredicate.js`))
    .then(() => done('fail'))
    .catch((e) => expect(e.message).equals('Filter predicate() not provided'))
    .then(() => done());
  });

  it('get ids in empty namespace', (done) => {
    filters.getIds(ns)
    .then((ids) => expect(ids).to.be.empty)
    .then(() => done());
  });

  it('basic crud', (done) => {
    filters.add(objectId, utils.loadFile(`${__dirname}/validFilters/index.js`))
    .then(() => filters.exists(objectId))
    .then((exists) => expect(exists).to.be.equals(1))
    .then(() => filters.getIds(ns)
        .then((ids) => {
          expect(ids).to.have.length(1);
          expect(ids[0]).to.be.equals('id');
        })
    )
    .then(() => filters.remove(objectId))
    .then(() => filters.exists(objectId))
    .then((exists) => expect(exists).to.be.equals(0))
    .then(() => filters.getIds(ns))
    .then((ids) => expect(ids).to.be.empty)
    .then(() => done());
  });

  it('add twice', (done) => {
    filters.add(objectId, utils.loadFile(`${__dirname}/validFilters/index.js`))
    .then(() => filters.exists(objectId))
    .then((exists) => expect(exists).to.be.equals(1))
    .then(() => filters.add(objectId, utils.loadFile(`${__dirname}/validFilters/index.js`)))
    .catch((e) => expect(e.message).to.be.equals('Filter \'namespace/id\' exists, delete first.'))
    .then(() => done());
  });

  it('load a specific filter with top-level args', (done) => {
    filters.add(objectId, utils.loadFile(`${__dirname}/validFilters/index.js`))
    .then(() => filters.load(ns, {
      actions: [
        {id}
      ],
      arguments: topLevelArguments
    }))
    .then((loadedFilters) => {
      expect(_.size(loadedFilters)).to.be.equals(1);
      expect(loadedFilters['index']).to.have.length(1);

      const data = loadedFilters['index'][0];
      expect(data.type).to.be.equals('index');
      expect(data.arguments).to.be.equals(topLevelArguments);
      validateFilter(data);
      done();
    })
    .catch(done);
  });

  it('load a specific filter with arg overrides', (done) => {
    filters.add(objectId, utils.loadFile(`${__dirname}/validFilters/index.js`))
    .then(() => filters.load(ns, {
      actions: [
        {id, arguments: specificArguments}
      ],
      arguments: topLevelArguments
    }))
    .then((loadedFilters) => {
      expect(_.size(loadedFilters)).to.be.equals(1);
      expect(loadedFilters['index']).to.have.length(1);

      const data = loadedFilters['index'][0];
      expect(data.type).to.be.equals('index');
      expect(data.arguments).to.be.equals(specificArguments);
      validateFilter(data);
      done();
    })
    .catch(done);
  });

  it('load a multiple filters in different namespaces', (done) => {
    const objectId1 = new ObjectId({namespace: 'task', id});
    const objectId2 = new ObjectId({namespace: 'global', id});
    const objectId3 = new ObjectId({namespace: 'othernamespace', id});
    const objectId4 = new ObjectId({namespace: 'othernamespace', id: 'other'});

    filters.add(objectId1, utils.loadFile(`${__dirname}/validFilters/index.js`))
    .then(() => filters.add(objectId2, utils.loadFile(`${__dirname}/validFilters/type.js`)))
    .then(() => filters.add(objectId3, utils.loadFile(`${__dirname}/validFilters/typeWithArgs.js`)))
    .then(() => filters.add(objectId4, utils.loadFile(`${__dirname}/validFilters/index.js`)))
    .then(() => filters.load('task', {
      actions: [
        {
          namespace: objectId3.namespace,
          id:        objectId3.id,
          arguments: specificArguments
        },
        {
          namespace: objectId2.namespace,
          id:        objectId2.id
        },
        {
          id:        objectId1.id,
          arguments: specificArguments
        },
        {
          namespace: objectId4.namespace,
          id:        objectId4.id
        }
      ],
      arguments: topLevelArguments
    }))
    .then((loadedFilters) => {
      expect(_.size(loadedFilters)).to.be.equals(2);
      expect(loadedFilters['index']).to.have.length(2);
      expect(loadedFilters['type']).to.have.length(2);

      const index1 = loadedFilters['index'][0];
      expect(index1.type).to.be.equals('index');
      expect(index1.arguments).to.be.equals(specificArguments);
      validateFilter(index1);

      const index2 = loadedFilters['index'][0];
      expect(index2.type).to.be.equals('index');
      expect(index2.arguments).to.be.equals(specificArguments);
      validateFilter(index2);

      const type1 = loadedFilters['type'][0];
      expect(type1.type).to.be.equals('type');
      expect(type1.arguments).to.be.equals(specificArguments);
      validateFilter(type1);

      const type2 = loadedFilters['type'][1];
      expect(type2.type).to.be.equals('type');
      expect(type2.arguments).to.be.equals(topLevelArguments);
      validateFilter(type2);
    })
    .then(() => done())
    .catch(done);
  });
});
