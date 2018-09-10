/*eslint no-magic-numbers: "off"*/
/*eslint no-invalid-this: "off"*/
const expect            = require('chai').expect;
const Promise           = require('bluebird');
const TestConfig        = require('../config');
const Utils             = require('../utils');
const Subtask           = require('../../app/models/subtask');
const Subtasks          = require('../../app/services/subtasks');
const Transfer          = require('../../app/services/transfer');
const createEsClient    = require('../../config/elasticsearch');
const createRedisClient = require('../../config/redis');
const config            = require('../../config/index');
const to_bytes          = require('../../config/utils').to_bytes;

const log = config.log;

Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection((error) => log.error('Likely error: ', error.stack));

describe('subtasks service', function () {
  this.timeout(100000);

  let source   = null;
  let redis    = null;
  let subtasks = null;
  let utils    = null;

  before((done) => {
    source = createEsClient(TestConfig.elasticsearch.source);
    redis = createRedisClient(TestConfig.redis.host, TestConfig.redis.port);
    subtasks = new Subtasks(redis);
    utils = new Utils();

    utils.deleteAllTemplates(source)
    .finally(() => utils.deleteAllIndices(source))
    .finally(() => redis.flushdb())
    .finally(() => done());
  });

  afterEach((done) => {
    utils.deleteAllTemplates(source)
    .finally(() => utils.deleteAllIndices(source))
    .finally(() => redis.flushdb())
    .finally(() => done());
  });

  const TEST_INDEX    = 'myindex1';
  const TEST_TYPE     = 'mytype1';
  const fakeTask      = {
    source:      TestConfig.elasticsearch.source,
    destination: TestConfig.elasticsearch.destination,
    transfer:    {
      documents: {
        fromIndices: TEST_INDEX
      }
    }
  };
  const loadedFilters = {};
  const createIndex   = (shards) => source.indices.create({
    index: TEST_INDEX,
    body:  {
      settings: {
        index: {
          number_of_shards:   shards,
          number_of_replicas: 0
        }
      },
      mappings: {[TEST_TYPE]: {_size: {enabled: true}}}
    }
  });

  const upload      = (maxIterations, chunkSize, minSize, maxSize) => {
    const buildBody      = () => {
      const body = [];
      for (let i = 0; i < chunkSize; i++) {
        body.push({index: {_index: TEST_INDEX, _type: TEST_TYPE}});
        body.push({size: minSize + Math.floor(Math.random() * (maxSize - minSize))});
      }
      return body;
    };
    const uploadInternal = (index) => index >= maxIterations
        ? null
        : source.bulk({refresh: true, body: buildBody()})
        .then(() => source.indices.refresh({index: TEST_INDEX}))
        .then(() => uploadInternal(index + 1));

    return uploadInternal(0);
  };
  const uploadExact = (chunkSize, size) => {
    const buildBody = () => {
      const body = [];
      for (let i = 0; i < chunkSize; i++) {
        body.push({index: {_index: TEST_INDEX, _type: TEST_TYPE}});
        body.push({size});
      }
      return body;
    };
    return source.bulk({refresh: true, body: buildBody()})
    .then(() => source.indices.refresh({index: TEST_INDEX}));
  };

  const assertSubtask = (subtask, flushSize, minSize, maxSize) => {
    expect(subtask.source).to.be.equals(fakeTask.source);
    expect(subtask.destination).to.be.equals(fakeTask.destination);
    expect(subtask.mutators).to.be.equals(fakeTask.mutators);
    expect(subtask.transfer.flushSize).to.be.equals(flushSize);
    expect(subtask.transfer.documents.index).to.be.equals(TEST_INDEX);
    expect(subtask.transfer.documents.type).to.be.equals(TEST_TYPE);
    expect(subtask.transfer.documents.minSize).to.be.equals(minSize);
    expect(subtask.transfer.documents.maxSize).to.be.equals(maxSize);

  };

  it('pick bounds - no records', (done) => {
    createIndex(1)
    .then(() => Transfer.getIndices(source, fakeTask.transfer.documents.fromIndices))
    .then((allIndices) => subtasks.filterDocumentSubtasks(fakeTask, allIndices, loadedFilters, 'size'))
    .then((actual) => {
      expect(actual.length).to.be.equals(1);
      assertSubtask(actual[0], Subtask.DEFAULT_FLUSH_SIZE, -1, -1);
    })
    .then(() => done())
    .catch(done);
  });

  it('pick bounds - all records same size', (done) => {
    createIndex(1)
    .then(() => uploadExact(100, to_bytes(100, 'B')))
    .then(() => Transfer.getIndices(source, fakeTask.transfer.documents.fromIndices))
    .then((allIndices) => subtasks.filterDocumentSubtasks(fakeTask, allIndices, loadedFilters, 'size'))
    .then((actual) => {
      expect(actual.length).to.be.equals(1);
      assertSubtask(actual[0], 524288, -1, -1);
    })
    .then(() => done())
    .catch(done);
  });

  it('pick bounds - nothing above 1KB', (done) => {
    createIndex(1)
    .then(() => upload(10, 2000, to_bytes(1, 'B'), to_bytes(1, 'KB')))
    .then(() => uploadExact(1, to_bytes(1, 'KB')))
    .then(() => Transfer.getIndices(source, fakeTask.transfer.documents.fromIndices))
    .then((allIndices) => subtasks.filterDocumentSubtasks(fakeTask, allIndices, loadedFilters, 'size'))
    .then((actual) => {
      expect(actual.length).to.be.equals(3);
      assertSubtask(actual[0], 85667, to_bytes(0, 'B'), to_bytes(613, 'B'));
      assertSubtask(actual[1], 57111, to_bytes(613, 'B'), to_bytes(919, 'B'));
      assertSubtask(actual[2], 51200, to_bytes(919, 'B'), to_bytes(1, 'KB') + 1);
    })
    .then(() => done())
    .catch(done);
  });

  it('pick bounds - nothing above 10KB', (done) => {
    createIndex(1)
    .then(() => upload(10, 1000, to_bytes(1, 'B'), to_bytes(1, 'KB')))
    .then(() => upload(10, 100, to_bytes(7, 'KB'), to_bytes(8, 'KB')))
    .then(() => upload(10, 1000, to_bytes(9.2, 'KB'), to_bytes(10, 'KB')))
    .then(() => uploadExact(1, to_bytes(6, 'KB')))
    .then(() => uploadExact(1, to_bytes(9, 'KB')))
    .then(() => uploadExact(1, to_bytes(10, 'KB')))
    .then(() => Transfer.getIndices(source, fakeTask.transfer.documents.fromIndices))
    .then((allIndices) => subtasks.filterDocumentSubtasks(fakeTask, allIndices, loadedFilters, 'size'))
    .then((actual) => {
      expect(actual.length).to.be.equals(3);
      assertSubtask(actual[0], 8533, 0, 6145);
      assertSubtask(actual[1], 5688, 6145, 9217);
      assertSubtask(actual[2], 5120, 9217, to_bytes(10, 'KB') + 1);
    })
    .then(() => done())
    .catch(done);
  });

  it('pick bounds - nothing above 10KB and 4 shards', (done) => {
    createIndex(4)
    .then(() => upload(10, 1000, to_bytes(1, 'B'), to_bytes(1, 'KB')))
    .then(() => upload(10, 100, to_bytes(7, 'KB'), to_bytes(8, 'KB')))
    .then(() => upload(10, 1000, to_bytes(9.2, 'KB'), to_bytes(10, 'KB')))
    .then(() => uploadExact(1, to_bytes(6, 'KB')))
    .then(() => uploadExact(1, to_bytes(9, 'KB')))
    .then(() => uploadExact(1, to_bytes(10, 'KB')))
    .then(() => Transfer.getIndices(source, fakeTask.transfer.documents.fromIndices))
    .then((allIndices) => subtasks.filterDocumentSubtasks(fakeTask, allIndices, loadedFilters, 'size'))
    .then((actual) => {
      expect(actual.length).to.be.equals(3);
      assertSubtask(actual[0], 2133, 0, 6145);
      assertSubtask(actual[1], 1422, 6145, 9217);
      assertSubtask(actual[2], 1280, 9217, to_bytes(10, 'KB') + 1);
    })
    .then(() => done())
    .catch(done);
  });

  it('pick bounds - nothing above 50MB', (done) => {
    createIndex(1)
    .then(() => upload(10, 1000, to_bytes(20, 'B'), to_bytes(10, 'KB')))
    .then(() => upload(50, 5000, to_bytes(10, 'KB'), to_bytes(200, 'KB')))
    .then(() => upload(1, 100, to_bytes(20, 'MB'), to_bytes(50, 'MB')))
    .then(() => uploadExact(1, to_bytes(50, 'MB')))
    .then(() => Transfer.getIndices(source, fakeTask.transfer.documents.fromIndices))
    .then((allIndices) => subtasks.filterDocumentSubtasks(fakeTask, allIndices, loadedFilters, 'size'))
    .then((actual) => {
      expect(actual.length).to.be.equals(2);
      assertSubtask(actual[0], 100, 0, 524288);
      assertSubtask(actual[1], 1, 1048576, to_bytes(50, 'MB') + 1);
    })
    .then(() => done())
    .catch(done);
  });

  it('pick bounds - nothing above 50MB', (done) => {
    createIndex(1)
    .then(() => upload(10, 1000, to_bytes(20, 'B'), to_bytes(10, 'KB')))
    .then(() => upload(50, 5000, to_bytes(10, 'KB'), to_bytes(200, 'KB')))
    .then(() => upload(1, 100, to_bytes(20, 'MB'), to_bytes(500, 'MB')))
    .then(() => uploadExact(1, to_bytes(500, 'MB')))
    .then(() => Transfer.getIndices(source, fakeTask.transfer.documents.fromIndices))
    .then((allIndices) => subtasks.filterDocumentSubtasks(fakeTask, allIndices, loadedFilters, 'size'))
    .then((actual) => {
      expect(actual.length).to.be.equals(2);
      assertSubtask(actual[0], 100, 0, 524288);
      assertSubtask(actual[1], 1, 1048576, to_bytes(500, 'MB') + 1);
    })
    .then(() => done())
    .catch(done);
  });
});
