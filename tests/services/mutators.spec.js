/*eslint no-magic-numbers: "off"*/
/*eslint no-invalid-this: "off"*/
const _                 = require('lodash');
const chai              = require('chai');
const expect            = chai.expect;
const asPromised        = require('chai-as-promised');
const Promise           = require('bluebird');
const TestConfig        = require('../config');
const Utils             = require('../utils');
const Mutators          = require('../../app/services/mutators');
const ObjectId          = require('../../app/models/objectId');
const createRedisClient = require('../../config/redis');
const config            = require('../../config/index');
chai.use(asPromised);

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
const validateMutator   = (mutator) => {
  expect(_.isFunction(mutator.predicate)).is.true;
  expect(_.isFunction(mutator.mutate)).is.true;
};

describe('mutators service', function () {
  this.timeout(5000);

  let redis    = null;
  let mutators = null;
  let objectId = null;
  let utils    = null;

  before((done) => {
    redis = createRedisClient(TestConfig.redis.host, TestConfig.redis.port);
    mutators = new Mutators(redis);
    utils = new Utils();
    done();
  });

  after((done) => {
    redis.quit().finally(() => done());
  });

  beforeEach((done) => {
    objectId = new ObjectId({namespace: ns, id: id});
    redis.flushdb()
      .finally(() => done());
  });

  it('invalid id', (done) => {
    objectId.id = '~badId';
    mutators.add(objectId, 'dummySrc')
      .then(() => done('fail'))
      .catch((e) => expect(e.message).equals("Id must be string of 1-40 alphanumeric characters, given '~badId'"))
      .then(() => done());
  });

  it('invalid src type', (done) => {
    mutators.add(objectId, {})
      .then(() => done('fail'))
      .catch((e) => expect(e.message).equals('mutatorSrc must be string'))
      .then(() => done());
  });

  it('invalid javascript', (done) => {
    mutators.add(objectId, utils.loadFile(`${__dirname}/invalidMutators/notAJsFile`))
      .then(() => done('fail'))
      .catch((e) => expect(e.message).equals("Unable to load external module due to 'SyntaxError - Unexpected identifier'"))
      .then(() => done());
  });

  it('mutator missing type', (done) => {
    mutators.add(objectId, utils.loadFile(`${__dirname}/invalidMutators/noType.js`))
      .then(() => done('fail'))
      .catch((e) => expect(e.message).equals('Mutator type string not provided'))
      .then(() => done());
  }
  );

  it('mutator invalid type', (done) => {
    mutators.add(objectId, utils.loadFile(`${__dirname}/invalidMutators/invalidType.js`))
      .then(() => done('fail'))
      .catch((e) => expect(e.message).equals(`Mutator type 'wrong' not one of: [${Mutators.TYPES}]`))
      .then(() => done());
  });

  it('mutator missing predicate', (done) => {
    mutators.add(objectId, utils.loadFile(`${__dirname}/invalidMutators/noPredicate.js`))
      .then(() => done('fail'))
      .catch((e) => expect(e.message).equals('Mutator predicate() not provided'))
      .then(() => done());
  });

  it('mutator missing mutate', (done) => {
    mutators.add(objectId, utils.loadFile(`${__dirname}/invalidMutators/noMutate.js`))
      .then(() => done('fail'))
      .catch((e) => expect(e.message).equals('Mutator mutate() not provided'))
      .then(() => done());
  });

  it('get ids in empty namespace', (done) => {
    mutators.getIds(ns)
      .then((ids) => expect(ids).to.be.empty)
      .then(() => done());
  });

  it('basic crud', (done) => {
    mutators.add(objectId, utils.loadFile(`${__dirname}/validMutators/data.js`))
      .then(() => mutators.exists(objectId))
      .then((exists) => expect(exists).to.be.equals(1))
      .then(() => mutators.getIds(ns)
        .then((ids) => {
          expect(ids).to.have.length(1);
          expect(ids[0]).to.be.equals('id');
        })
      )
      .then(() => mutators.remove(objectId))
      .then(() => mutators.exists(objectId))
      .then((exists) => expect(exists).to.be.equals(0))
      .then(() => mutators.getIds(ns))
      .then((ids) => expect(ids).to.be.empty)
      .then(() => done());
  });

  it('add twice', (done) => {
    mutators.add(objectId, utils.loadFile(`${__dirname}/validMutators/data.js`))
      .then(() => mutators.exists(objectId))
      .then((exists) => expect(exists).to.be.equals(1))
      .then(() => mutators.add(objectId, utils.loadFile(`${__dirname}/validMutators/data.js`)))
      .catch((e) => expect(e.message).to.be.equals('Mutator \'namespace/id\' exists, delete first.'))
      .then(() => done());
  });

  it('load a specific mutator with top-level args', (done) => {
    mutators.add(objectId, utils.loadFile(`${__dirname}/validMutators/data.js`))
      .then(() => mutators.load(ns, {
        actions: [
          {id: id}
        ],
        arguments: topLevelArguments
      }))
      .then((loadedMutators) => {
        expect(_.size(loadedMutators)).to.be.equals(1);
        expect(loadedMutators['data']).to.have.length(1);

        const data = loadedMutators['data'][0];
        expect(data.type).to.be.equals('data');
        expect(data.arguments).to.be.equals(topLevelArguments);
        validateMutator(data);
        done();
      })
      .catch(done);
  });

  it('load a specific mutator with arg overrides', (done) => {
    mutators.add(objectId, utils.loadFile(`${__dirname}/validMutators/data.js`))
      .then(() => mutators.load(ns, {
        actions: [
          {
            id:        id,
            arguments: specificArguments
          }
        ],
        arguments: topLevelArguments
      }))
      .then((loadedMutators) => {
        expect(_.size(loadedMutators)).to.be.equals(1);
        expect(loadedMutators['data']).to.have.length(1);

        const data = loadedMutators['data'][0];
        expect(data.type).to.be.equals('data');
        expect(data.arguments).to.be.equals(specificArguments);
        validateMutator(data);
        done();
      })
      .catch(done);
  });

  it('load a multiple mutators in different namespaces', (done) => {
    const objectId1 = new ObjectId({namespace: 'task', id: id});
    const objectId2 = new ObjectId({namespace: 'global', id: id});
    const objectId3 = new ObjectId({namespace: 'othernamespace', id: id});
    const objectId4 = new ObjectId({namespace: 'othernamespace', id: 'other'});

    mutators.add(objectId1, utils.loadFile(`${__dirname}/validMutators/data.js`))
      .then(() => mutators.add(objectId2, utils.loadFile(`${__dirname}/validMutators/index.js`)))
      .then(() => mutators.add(objectId3, utils.loadFile(`${__dirname}/validMutators/indexWithArgs.js`)))
      .then(() => mutators.add(objectId4, utils.loadFile(`${__dirname}/validMutators/template.js`)))
      .then(() => mutators.load('task', {
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
      .then((loadedMutators) => {
        expect(_.size(loadedMutators)).to.be.equals(3);
        expect(loadedMutators['data']).to.have.length(1);
        expect(loadedMutators['index']).to.have.length(2);
        expect(loadedMutators['template']).to.have.length(1);

        const data = loadedMutators['data'][0];
        expect(data.type).to.be.equals('data');
        expect(data.arguments).to.be.equals(specificArguments);
        validateMutator(data);

        const index1 = loadedMutators['index'][0];
        expect(index1.type).to.be.equals('index');
        expect(index1.arguments).to.be.equals(specificArguments);
        validateMutator(index1);

        const index2 = loadedMutators['index'][1];
        expect(index2.type).to.be.equals('index');
        expect(index2.arguments).to.be.equals(topLevelArguments);
        validateMutator(index2);

        const template = loadedMutators['template'][0];
        expect(template.type).to.be.equals('template');
        expect(template.arguments).to.be.equals(topLevelArguments);
        validateMutator(template);
        done();
      })
      .catch(done);
  });
});
