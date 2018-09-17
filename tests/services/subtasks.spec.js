/*eslint no-magic-numbers: "off"*/
/*eslint no-invalid-this: "off"*/
const _                 = require('lodash');
const expect            = require('chai').expect;
const Promise           = require('bluebird');
const TestConfig        = require('../config');
const Utils             = require('../utils');
const Subtasks          = require('../../app/services/subtasks');
const Tasks             = require('../../app/services/tasks');
const createEsClient    = require('../../config/elasticsearch');
const createRedisClient = require('../../config/redis');
const config            = require('../../config/index');

const log = config.log;

Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection((error) => log.error('Likely error: ', error.stack));

const TASK_NAME        = 'expectedTask';
const MOCK_ALL_INDICES = [
  {
    name:     'index_number_1',
    aliases:  {},
    mappings: {
      newtype_2: {
        properties: {
          something: {
            type: 'string'
          }
        }
      },
      newtype: {
        properties: {
          something2: {
            type: 'string'
          }
        }
      },
      oldtype: {
        properties: {
          something3: {
            type: 'string'
          }
        }
      }
    },
    settings: {
      index: {
        creation_date:      1461452305587,
        number_of_shards:   5,
        number_of_replicas: 1,
        uuid:               'wTr18aKmRKWqV80B62ZEJA',
        version:            {
          created: 2020299
        }
      }
    },
    warmers: {}
  },
  {
    name:     'index_number_2',
    aliases:  {},
    mappings: {
      newtype_3: {
        properties: {
          something: {
            type: 'string'
          }
        }
      },
      newtype: {
        properties: {
          something: {
            type: 'string'
          }
        }
      }
    },
    settings: {
      index: {
        creation_date:      1461452305587,
        number_of_shards:   5,
        number_of_replicas: 1,
        uuid:               'wTr18aKmRKWqV80B62ZEJA',
        version:            {
          created: 2020299
        }
      }
    },
    warmers: {}
  }
];

describe('subtasks service', function () {
  this.timeout(5000);

  let source   = null;
  let redis    = null;
  let tasks    = null;
  let subtasks = null;
  let utils    = null;

  before((done) => {
    source = createEsClient(TestConfig.elasticsearch.source);
    redis = createRedisClient(TestConfig.redis.host, TestConfig.redis.port);
    tasks = new Tasks(redis);
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

  it('should get subtasks in the same order they were added', (done) => {
    const expected = [
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count: 10
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex2',
            type:  'mytype1'
          }
        },
        count: 20
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype4'
          }
        },
        count: 1
      }
    ];

    subtasks.queue(TASK_NAME, expected)
    .then(() => subtasks.fetch(TASK_NAME))
    .then((subtask) => {
      expect(subtask.transfer.documents.index).to.be.equals(expected[0].transfer.documents.index);
      expect(subtask.transfer.documents.type).to.be.equals(expected[0].transfer.documents.type);
      expect(subtask.count).to.be.equals(expected[0].count);
    })
    .then(() => subtasks.fetch(TASK_NAME))
    .then((subtask) => {
      expect(subtask.transfer.documents.index).to.be.equals(expected[1].transfer.documents.index);
      expect(subtask.transfer.documents.type).to.be.equals(expected[1].transfer.documents.type);
      expect(subtask.count).to.be.equals(expected[1].count);
    })
    .then(() => subtasks.fetch(TASK_NAME))
    .then((subtask) => {
      expect(subtask.transfer.documents.index).to.be.equals(expected[2].transfer.documents.index);
      expect(subtask.transfer.documents.type).to.be.equals(expected[2].transfer.documents.type);
      expect(subtask.count).to.be.equals(expected[2].count);
    })
    .then(() => subtasks.fetch(TASK_NAME))
    .then((subtask) => expect(subtask).to.be.null)
    .then(() => done())
    .catch(done);
  });

  it('should not add the same subtask twice', (done) => {
    const expected = [
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count: 22
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count: 22
      }
    ];

    subtasks.queue(TASK_NAME, expected)
    .then(() => subtasks.fetch(TASK_NAME))
    .then((subtask) => {
      expect(subtask.transfer.documents.index).to.be.equals(expected[0].transfer.documents.index);
      expect(subtask.transfer.documents.type).to.be.equals(expected[0].transfer.documents.type);
      expect(subtask.count).to.be.equals(expected[0].count);
    })
    .then(() => subtasks.fetch(TASK_NAME))
    .then((subtask) => expect(subtask).to.be.null)
    .then(() => done())
    .catch(done);
  });

  it('should get all completed subtasks', (done) => {
    const expected = [
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count: 10
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex2',
            type:  'mytype1'
          }
        },
        count: 20
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype4'
          }
        },
        count: 1
      }
    ];

    Promise.each(expected, (subtask) => subtasks.complete(TASK_NAME, subtask))
    .then(() => subtasks.getCompleted(TASK_NAME))
    .then((completedSubtasks) => {
      let target = _.find(completedSubtasks, {count: expected[0].count});
      expect(target.transfer.documents.index).to.be.equals(expected[0].transfer.documents.index);
      expect(target.transfer.documents.type).to.be.equals(expected[0].transfer.documents.type);
      expect(target.count).to.be.equals(expected[0].count);

      target = _.find(completedSubtasks, {count: expected[1].count});
      expect(target.transfer.documents.index).to.be.equals(expected[1].transfer.documents.index);
      expect(target.transfer.documents.type).to.be.equals(expected[1].transfer.documents.type);
      expect(target.count).to.be.equals(expected[1].count);

      target = _.find(completedSubtasks, {count: expected[2].count});
      expect(target.transfer.documents.index).to.be.equals(expected[2].transfer.documents.index);
      expect(target.transfer.documents.type).to.be.equals(expected[2].transfer.documents.type);
      expect(target.count).to.be.equals(expected[2].count);
    })
    .then(() => done())
    .catch(done);
  });

  it('should get completed subtask count', (done) => {
    const expected = [
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count: 10
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex2',
            type:  'mytype1'
          }
        },
        count: 20
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype4'
          }
        },
        count: 1
      }
    ];

    Promise.each(expected, (subtask) => subtasks.complete(TASK_NAME, subtask))
    .then(() => subtasks.countCompleted(TASK_NAME))
    .then((completedCount) => expect(completedCount).to.be.equals(31))
    .then(() => done())
    .catch(done);
  });

  it('should clear completed subtasks', (done) => {
    const expected = [
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count: 10
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex2',
            type:  'mytype1'
          }
        },
        count: 20
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype4'
          }
        },
        count: 1
      }
    ];

    Promise.each(expected, (subtask) => subtasks.complete(TASK_NAME, subtask))
    .then(() => subtasks.clearCompleted(TASK_NAME))
    .then(() => subtasks.countCompleted(TASK_NAME))
    .then((completedCount) => expect(completedCount).to.be.equals(0))
    .then(() => done())
    .catch(done);
  });

  it('should return an empty array when there are no completed subtasks', (done) => {
    subtasks.getCompleted(TASK_NAME)
    .then((completedSubtasks) => expect(completedSubtasks).to.be.empty)
    .then(() => done())
    .catch(done);
  });

  it('should return an empty array when there are no backlog subtasks', (done) => {
    subtasks.getBacklog(TASK_NAME)
    .then((backlogSubtasks) => expect(backlogSubtasks).to.be.empty)
    .then(() => done())
    .catch(done);
  });

  it('should return all backlog subtasks', (done) => {
    const expected = [
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count: 10
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex2',
            type:  'mytype1'
          }
        },
        count: 20
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype4'
          }
        },
        count: 1
      }
    ];

    subtasks.queue(TASK_NAME, expected)
    .then(() => subtasks.getBacklog(TASK_NAME))
    .then((backlogSubtasks) => {
      let target = _.find(backlogSubtasks, {count: expected[0].count});
      expect(target.transfer.documents.index).to.be.equals(expected[0].transfer.documents.index);
      expect(target.transfer.documents.type).to.be.equals(expected[0].transfer.documents.type);
      expect(target.count).to.be.equals(expected[0].count);

      target = _.find(backlogSubtasks, {count: expected[1].count});
      expect(target.transfer.documents.index).to.be.equals(expected[1].transfer.documents.index);
      expect(target.transfer.documents.type).to.be.equals(expected[1].transfer.documents.type);
      expect(target.count).to.be.equals(expected[1].count);

      target = _.find(backlogSubtasks, {count: expected[2].count});
      expect(target.transfer.documents.index).to.be.equals(expected[2].transfer.documents.index);
      expect(target.transfer.documents.type).to.be.equals(expected[2].transfer.documents.type);
      expect(target.count).to.be.equals(expected[2].count);
    })
    .then(() => done())
    .catch(done);
  });

  it('should clear all backlog subtasks', (done) => {
    const expected = [
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count: 10
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex2',
            type:  'mytype1'
          }
        },
        count: 20
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype4'
          }
        },
        count: 1
      }
    ];

    subtasks.queue(TASK_NAME, expected)
    .then(() => subtasks.clearBacklog(TASK_NAME))
    .then(() => subtasks.getBacklog(TASK_NAME))
    .then((backlogSubtasks) => expect(backlogSubtasks).to.be.empty)
    .then(() => done())
    .catch(done);
  });

  it('should return count of zero for empty completed', (done) => {
    subtasks.countCompleted(TASK_NAME)
    .then((count) => expect(count).to.be.equals(0))
    .then(() => done())
    .catch(done);
  });

  it('should return count of zero for empty backlog', (done) => {
    subtasks.countBacklog(TASK_NAME)
    .then((count) => expect(count).to.be.equals(0))
    .then(() => done())
    .catch(done);
  });

  it('should return total count of subtasks in backlog', (done) => {
    const expected = [
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count: 10
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex2',
            type:  'mytype1'
          }
        },
        count: 20
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype4'
          }
        },
        count: 1
      }
    ];

    subtasks.queue(TASK_NAME, expected)
    .then(() => subtasks.countBacklog(TASK_NAME))
    .then((backlogTotal) => expect(backlogTotal).to.be.equals(expected.reduce((total, subtask) => total + subtask.count, 0)))
    .then(() => done())
    .catch(done);
  });

  it('should get counts for provided jobs', (done) => {
    const expected = [
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        }
      },
      {
        source:      TestConfig.elasticsearch.source,
        destination: TestConfig.elasticsearch.destination,
        transfer:    {
          documents: {
            index: 'myindex3',
            type:  'mytype3'
          }
        }
      }
    ];

    utils.addData(source)
    .then(() => Promise.map(expected, (subtask) => subtasks.addCount(source, subtask)))
    .then((subtasksWithCount) => {
      expect(subtasksWithCount.length).to.be.equals(2);

      let filter = {transfer: {documents: {index: 'myindex1'}}};
      let target = _.find(subtasksWithCount, filter);
      expect(target.count).to.be.equals(2);

      filter = {transfer: {documents: {index: 'myindex3'}}};
      target = _.find(subtasksWithCount, filter);
      expect(target.count).to.be.equals(3);
    })
    .then(() => done())
    .catch(done);
  });

  it('should filter out documents by index regex', (done) => {
    const fakeTask = {
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      mutators:    'path/to/mutators'
    };

    const filterFunctions = {
      index: [
        {
          predicate: (index) => index.name === 'index_number_1'
        }
      ]
    };

    subtasks.filterDocumentSubtasks(fakeTask, MOCK_ALL_INDICES, filterFunctions)
    .then((actual) => {
      expect(actual.length).to.be.equals(3);
      expect(actual[0].source).to.be.equals(fakeTask.source);
      expect(actual[0].destination).to.be.equals(fakeTask.destination);
      expect(actual[0].mutators).to.be.equals(fakeTask.mutators);
      expect(actual[0].transfer.documents.index).to.be.equals('index_number_1');
      expect(actual[0].transfer.documents.type).to.be.oneOf([
        'newtype',
        'oldtype',
        'newtype_2'
      ]);

      expect(actual[1].source).to.be.equals(fakeTask.source);
      expect(actual[1].destination).to.be.equals(fakeTask.destination);
      expect(actual[1].mutators).to.be.equals(fakeTask.mutators);
      expect(actual[1].transfer.documents.index).to.be.equals('index_number_1');
      expect(actual[1].transfer.documents.type).to.be.oneOf([
        'newtype',
        'oldtype',
        'newtype_2'
      ]);

      expect(actual[2].source).to.be.equals(fakeTask.source);
      expect(actual[2].destination).to.be.equals(fakeTask.destination);
      expect(actual[2].mutators).to.be.equals(fakeTask.mutators);
      expect(actual[2].transfer.documents.index).to.be.equals('index_number_1');
      expect(actual[2].transfer.documents.type).to.be.oneOf([
        'newtype',
        'oldtype',
        'newtype_2'
      ]);
    })
    .then(() => done())
    .catch(done);
  });

  it('should filter out documents by type regex', (done) => {
    const fakeTask = {
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      mutators:    'path/to/mutators'
    };

    const filterFunctions = {
      type: [
        {
          predicate: (type) => type.name === 'newtype'
        }
      ]
    };

    subtasks.filterDocumentSubtasks(fakeTask, MOCK_ALL_INDICES, filterFunctions)
    .then((actual) => {
      expect(actual.length).to.be.equals(2);
      expect(actual[0].source).to.be.equals(fakeTask.source);
      expect(actual[0].destination).to.be.equals(fakeTask.destination);
      expect(actual[0].mutators).to.be.equals(fakeTask.mutators);
      expect(actual[0].transfer.documents.index).to.be.oneOf([
        'index_number_1',
        'index_number_2'
      ]);
      expect(actual[0].transfer.documents.type).to.be.equals('newtype');

      expect(actual[1].source).to.be.equals(fakeTask.source);
      expect(actual[1].destination).to.be.equals(fakeTask.destination);
      expect(actual[1].mutators).to.be.equals(fakeTask.mutators);
      expect(actual[1].transfer.documents.index).to.be.oneOf([
        'index_number_1',
        'index_number_2'
      ]);
      expect(actual[1].transfer.documents.type).to.be.equals('newtype');
    })
    .then(() => done())
    .catch(done);
  });

  it('should prep subtasks backlog considering completed jobs', (done) => {
    const completedSubtask = {
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      transfer:    {
        documents: {
          index: 'myindex1',
          type:  'mytype1'
        }
      },
      count: 10
    };

    const taskParams = {
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      transfer:    {
        documents: {
          fromIndices: '*'
        }
      }
    };

    source.indices.create({index: 'myindex1'})
    .then(() => source.bulk({
      refresh: true,
      body:    [
        {
          index: {
            _index: 'myindex1',
            _type:  'mytype1'
          }
        },
        {someField1: 'somedata1'},
        {
          index: {
            _index: 'myindex1',
            _type:  'mytype1'
          }
        },
        {someField1: 'somedata3'}
      ]
    }))
    .then(() => subtasks.complete(TASK_NAME, completedSubtask))
    .then(() => subtasks.buildBacklog(TASK_NAME, taskParams))
    .then(() => subtasks.fetch(TASK_NAME))
    .then((subtask) => expect(subtask).to.be.null)
    .then(() => done())
    .catch(done);
  });

  it('should prep job backlog with no completed jobs', (done) => {
    const taskParams = {
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      transfer:    {
        documents: {
          fromIndices: '*'
        }
      }
    };

    source.indices.create({index: 'myindex1'})
    .then(() => source.bulk({
      refresh: true,
      body:    [
        {
          index: {
            _index: 'myindex1',
            _type:  'mytype1'
          }
        },
        {someField1: 'somedata1'},
        {
          index: {
            _index: 'myindex1',
            _type:  'mytype1'
          }
        },
        {someField1: 'somedata3'}
      ]
    }))
    .then(() => subtasks.buildBacklog(TASK_NAME, taskParams))
    .then(() => subtasks.fetch(TASK_NAME))
    .then((subtask) => {
      expect(subtask.transfer.documents.index).to.be.equals('myindex1');
      expect(subtask.transfer.documents.type).to.be.equals('mytype1');
    })
    .then(() => subtasks.fetch(TASK_NAME))
    .then((subtask) => expect(subtask).to.be.null)
    .then(() => done())
    .catch(done);
  });

  it('should keep track of progress for a single task', (done) => {
    const subtask = {
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      transfer:    {
        documents: {
          index: 'myindex1',
          type:  'mytype1'
        }
      },
      count: 10
    };

    const progressUpdate = {
      tick:        10,
      total:       20,
      transferred: 10
    };

    subtasks.updateProgress(TASK_NAME, subtask, progressUpdate)
    .then(() => subtasks.getProgress(TASK_NAME, subtask))
    .then((progress) => {
      expect(progress.tick).to.be.equals(10);
      expect(progress.total).to.be.equals(20);
      expect(progress.transferred).to.be.equals(10);
      expect(progress.lastModified).to.not.be.undefined;

      progressUpdate.tick = 5;
      progressUpdate.transferred = 15;
    })
    .then(() => subtasks.updateProgress(TASK_NAME, subtask, progressUpdate))
    .then(() => subtasks.getProgress(TASK_NAME, subtask))
    .then((progress) => {
      expect(progress.tick).to.be.equals(5);
      expect(progress.total).to.be.equals(20);
      expect(progress.transferred).to.be.equals(15);
      expect(progress.lastModified).to.not.be.undefined;
    })
    .then(() => done())
    .catch(done);
  });

  it('should keep track of progress for multiple tasks', (done) => {
    const subtask1 = {
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      transfer:    {
        documents: {
          index: 'myindex1',
          type:  'mytype1'
        }
      },
      count: 10
    };

    const subtask2 = {
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      transfer:    {
        documents: {
          index: 'myindex3',
          type:  'mytype1'
        }
      },
      count: 25
    };

    const progressUpdate = {
      tick:        10,
      total:       20,
      transferred: 10
    };

    subtasks.updateProgress(TASK_NAME, subtask1, progressUpdate)
    .then(() => subtasks.getProgress(TASK_NAME, subtask1))
    .then((progress) => {
      expect(progress.tick).to.be.equals(10);
      expect(progress.total).to.be.equals(20);
      expect(progress.transferred).to.be.equals(10);
      expect(progress.lastModified).to.not.be.undefined;

      progressUpdate.tick = 5;
      progressUpdate.transferred = 15;
    })
    .then(() => subtasks.updateProgress(TASK_NAME, subtask2, progressUpdate))
    .then(() => subtasks.getProgress(TASK_NAME, subtask2))
    .then((progress) => {
      expect(progress.tick).to.be.equals(5);
      expect(progress.total).to.be.equals(20);
      expect(progress.transferred).to.be.equals(15);
      expect(progress.lastModified).to.not.be.undefined;
    })
    .then(() => tasks.getProgress(TASK_NAME))
    .then((overallProgress) => {
      expect(overallProgress.length).to.be.equals(2);

      const predicate = {
        subtask: {
          transfer: {
            documents: {
              index: 'myindex1'
            }
          }
        }
      };
      let target      = _.find(overallProgress, predicate);
      expect(target.progress.tick).to.be.equals(10);

      predicate.subtask.transfer.documents.index = 'myindex3';

      target = _.find(overallProgress, predicate);
      expect(target.progress.tick).to.be.equals(5);
    })
    .then(() => done())
    .catch(done);
  });

  it('should delete progress of specific subtask', (done) => {
    const subtask1 = {
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      transfer:    {
        documents: {
          index: 'myindex1',
          type:  'mytype1'
        }
      },
      count: 10
    };

    const subtask2 = {
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      transfer:    {
        documents: {
          index: 'myindex3',
          type:  'mytype1'
        }
      },
      count: 25
    };

    const progressUpdate = {
      tick:        10,
      total:       20,
      transferred: 10
    };

    subtasks.updateProgress(TASK_NAME, subtask1, progressUpdate)
    .then(() => subtasks.getProgress(TASK_NAME, subtask1))
    .then(() => {
      progressUpdate.tick = 5;
      progressUpdate.transferred = 15;
    })
    .then(() => subtasks.updateProgress(TASK_NAME, subtask2, progressUpdate))
    .then(() => subtasks.getProgress(TASK_NAME, subtask2))
    .then(() => tasks.getProgress(TASK_NAME))
    .then((overallProgress) => expect(overallProgress.length).to.be.equals(2))
    .then(() => subtasks.removeProgress(TASK_NAME, subtask1))
    .then(() => tasks.getProgress(TASK_NAME))
    .then((overallProgress) => {
      expect(overallProgress.length).to.be.equals(1);
      expect(overallProgress[0].subtask.transfer.documents.index).to.be.equals('myindex3');
      expect(overallProgress[0].progress.tick).to.be.equals(5);
    })
    .then(() => done())
    .catch(done);
  });
});
