/*eslint no-magic-numbers: "off"*/
/*eslint no-invalid-this: "off"*/
const expect            = require('chai').expect;
const Subtask           = require('../../app/models/subtask');
const Manager           = require('../../app/services/manager');
const config            = require('../../config/index');
const createEsClient    = require('../../config/elasticsearch.js');
const createRedisClient = require('../../config/redis');
const _                 = require('lodash');
const path              = require('path');

const log = config.log;

const Promise = require('bluebird');
Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection((error) => {
  log.error('Likely error: ', error.stack);
});

describe('job manager', () => {
  // this.timeout(8000);

  let manager = null;
  let source  = null;
  let redis   = null;

  const SOURCE_ES_HOST = 'localhost:9200';
  const DEST_ES_HOST   = 'localhost:9201';

  before((done)=> {
    source  = createEsClient(SOURCE_ES_HOST, '1.4');
    redis   = createRedisClient('localhost', 6379);
    manager = new Manager(redis);

    source.indices.deleteTemplate({name: '*'}).finally(()=> {
      return source.indices.delete({index: '*'});
    }).finally(()=> {
      return redis.flushdb();
    }).finally(()=> {
      done();
    });
  });

  const TASK_NAME = 'testTask';

  const allIndices = [
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
        newtype:   {
          properties: {
            something2: {
              type: 'string'
            }
          }
        },
        oldtype:   {
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
      warmers:  {}
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
        newtype:   {
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
      warmers:  {}
    }
  ];

  const addData = (client)=> {
    return client.bulk({
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
            _type:  'mytype2'
          }
        },
        {someField2: 'somedata2'},
        {
          index: {
            _index: 'myindex2',
            _type:  'mytype1'
          }
        },
        {someField1: 'somedata1'},
        {
          index: {
            _index: 'myindex3',
            _type:  'mytype2'
          }
        },
        {someField2: 'somedata2'},
        {
          index: {
            _index: 'myindex3',
            _type:  'mytype3'
          }
        },
        {someField2: 'somedata3'},
        {
          index: {
            _index: 'myindex3',
            _type:  'mytype3'
          }
        },
        {someField2: 'somedata3'}
      ]
    });
  };

  it('should get all indices and types', (done)=> {
    addData(source).then(()=> {
      return source.indices.refresh();
    }).then(()=> {
      return manager.getIndices(source, '*');
    }).then((indices)=> {
      expect(_.size(indices)).to.eql(3);

      const myindex1 = _.find(indices, {name: 'myindex1'});
      expect(myindex1).to.be.defined;
      expect(_.size(myindex1.mappings)).to.eql(2);
      expect(myindex1.mappings.mytype1).to.be.defined;
      expect(myindex1.mappings.mytype2).to.be.defined;

      const myindex2 = _.find(indices, {name: 'myindex2'});
      expect(myindex2).to.be.defined;
      expect(_.size(myindex2.mappings)).to.eql(1);
      expect(myindex2.mappings.mytype1).to.be.defined;

      const myindex3 = _.find(indices, {name: 'myindex3'});
      expect(myindex3).to.be.defined;
      expect(_.size(myindex1.mappings)).to.eql(2);
      expect(myindex3.mappings.mytype2).to.be.defined;
      expect(myindex3.mappings.mytype3).to.be.defined;

      done();
    }).catch(done);
  });

  it('should create filter functions from regex', ()=> {
    const filterSpec = {
      indices: {
        type:  'regex',
        value: '^something$'
      },
      types:   {
        type:  'regex',
        value: '^nothing$'
      }
    };

    const filters = manager.createFilterFunctions(filterSpec);

    expect(_.size(filters)).to.eql(2);
    expect(filters.indices({name: 'something'})).to.eql(true);
    expect(filters.indices({name: 'something else'})).to.eql(false);

    expect(filters.types({name: 'nothing'})).to.eql(true);
    expect(filters.types({name: 'nothing else'})).to.eql(false);
  });

  it('should not create filter functions from type other than regex or path', ()=> {
    let filterSpec = {
      indices: {
        type:  'notPath',
        value: '^something$'
      }
    };

    const throws = () => {
      manager.createFilterFunctions(filterSpec);
    };

    expect(throws).to.throw(/Unexpected filter type/);

    filterSpec = {
      types: {
        type:  'notPath',
        value: '^something$'
      }
    };

    expect(throws).to.throw(/Unexpected filter type/);
  });

  it('should pass along arguments to filter', () => {
    const filterSpec = {
      types:   {
        type:  'path',
        value: path.join(__dirname, 'testFilters/argFilter.js'),
        arguments: {
          fieldValue: 'match value'
        }
      }
    };

    const filters = manager.createFilterFunctions(filterSpec);

    expect(filters.types({field: 'match value'})).to.eql(true);
    expect(filters.types({field: 'not match'})).to.eql(false);
  });

  it('should create filter functions from paths', ()=> {
    const filterSpec = {
      indices: {
        type:  'path',
        value: path.join(__dirname, 'testFilters/indexFilter.js')
      },
      types:   {
        type:  'path',
        value: path.join(__dirname, 'testFilters/typeFilter.js')
      }
    };

    const filters = manager.createFilterFunctions(filterSpec);

    expect(_.size(filters)).to.eql(2);
    expect(filters.indices({field: 'target index'})).to.eql(true);
    expect(filters.indices({field: 'not target'})).to.eql(false);

    expect(filters.types({field: 'target type'})).to.eql(true);
    expect(filters.types({field: 'not target'})).to.eql(false);
  });

  it('should filter out documents by index regex', ()=> {
    const fakeTask = {
      source:      {
        host:       SOURCE_ES_HOST,
        apiVersion: '1.4'
      },
      destination: {
        host:       DEST_ES_HOST,
        apiVersion: '2.2'
      },
      mutators:    'path/to/mutators'
    };

    const filterFunctions = {
      indices: (index) => {
        return index.name === 'index_number_1';
      }
    };

    const subtasks = manager.filterDocumentSubtasks(fakeTask, allIndices, filterFunctions);
    expect(subtasks.length).to.eql(3);
    expect(subtasks[0].source).to.eql(fakeTask.source);
    expect(subtasks[0].destination).to.eql(fakeTask.destination);
    expect(subtasks[0].mutators).to.eql(fakeTask.mutators);
    expect(subtasks[0].transfer.documents.index).to.eql('index_number_1');
    expect(subtasks[0].transfer.documents.type).to.be.oneOf([
      'newtype',
      'oldtype',
      'newtype_2'
    ]);

    expect(subtasks[1].source).to.eql(fakeTask.source);
    expect(subtasks[1].destination).to.eql(fakeTask.destination);
    expect(subtasks[1].mutators).to.eql(fakeTask.mutators);
    expect(subtasks[1].transfer.documents.index).to.eql('index_number_1');
    expect(subtasks[1].transfer.documents.type).to.be.oneOf([
      'newtype',
      'oldtype',
      'newtype_2'
    ]);

    expect(subtasks[2].source).to.eql(fakeTask.source);
    expect(subtasks[2].destination).to.eql(fakeTask.destination);
    expect(subtasks[2].mutators).to.eql(fakeTask.mutators);
    expect(subtasks[2].transfer.documents.index).to.eql('index_number_1');
    expect(subtasks[2].transfer.documents.type).to.be.oneOf([
      'newtype',
      'oldtype',
      'newtype_2'
    ]);
  });

  it('should filter out documents by type regex', ()=> {
    const fakeTask = {
      source:      {
        host:       SOURCE_ES_HOST,
        apiVersion: '1.4'
      },
      destination: {
        host:       DEST_ES_HOST,
        apiVersion: '2.2'
      },
      mutators:    'path/to/mutators'
    };

    const filterFunctions = {
      types: (type) => {
        return type.name === 'newtype';
      }
    };

    const subtasks = manager.filterDocumentSubtasks(fakeTask, allIndices, filterFunctions);

    expect(subtasks.length).to.eql(2);
    expect(subtasks[0].source).to.eql(fakeTask.source);
    expect(subtasks[0].destination).to.eql(fakeTask.destination);
    expect(subtasks[0].mutators).to.eql(fakeTask.mutators);
    expect(subtasks[0].transfer.documents.index).to.be.oneOf([
      'index_number_1',
      'index_number_2'
    ]);
    expect(subtasks[0].transfer.documents.type).to.eql('newtype');

    expect(subtasks[1].source).to.eql(fakeTask.source);
    expect(subtasks[1].destination).to.eql(fakeTask.destination);
    expect(subtasks[1].mutators).to.eql(fakeTask.mutators);
    expect(subtasks[1].transfer.documents.index).to.be.oneOf([
      'index_number_1',
      'index_number_2'
    ]);
    expect(subtasks[1].transfer.documents.type).to.eql('newtype');
  });

  it('should prep subtasks backlog considering completed jobs', (done)=> {
    const completedSubtask = {
      source:      {
        host:       SOURCE_ES_HOST,
        apiVersion: '1.4'
      },
      destination: {
        host:       DEST_ES_HOST,
        apiVersion: '2.2'
      },
      transfer:    {
        documents: {
          index: 'myindex1',
          type:  'mytype1'
        }
      },
      count:       10
    };

    const taskParams = {
      source:      {
        host:       SOURCE_ES_HOST,
        apiVersion: '1.4'
      },
      destination: {
        host:       DEST_ES_HOST,
        apiVersion: '2.2'
      },
      transfer:        {
        documents: {
          fromIndices: '*'
        }
      }
    };

    manager.completeSubtask(TASK_NAME, completedSubtask).then(()=> {
      return source.bulk({
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
              _type:  'mytype2'
            }
          },
          {someField1: 'somedata3'}
        ]
      });
    }).then(()=> {
      return manager.buildSubtaskBacklog(TASK_NAME, taskParams);
    }).then(()=> {
      return manager.fetchSubtask(TASK_NAME);
    }).then((subtask)=> {
      expect(subtask.transfer.documents.index).to.eql('myindex1');
      expect(subtask.transfer.documents.type).to.eql('mytype2');
      return manager.fetchSubtask(TASK_NAME);
    }).then((subtask)=> {
      expect(subtask).to.be.null;
      done();
    }).catch(done);
  });

  it('should prep job backlog with no completed jobs', (done)=> {
    const taskParams = {
      source:      {
        host:       SOURCE_ES_HOST,
        apiVersion: '1.4'
      },
      destination: {
        host:       DEST_ES_HOST,
        apiVersion: '2.2'
      },
      transfer:        {
        documents: {
          fromIndices: '*'
        }
      }
    };

    source.bulk({
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
            _type:  'mytype2'
          }
        },
        {someField1: 'somedata3'}
      ]
    }).then(()=> {
      return manager.buildSubtaskBacklog(TASK_NAME, taskParams);
    }).then(()=> {
      return manager.fetchSubtask(TASK_NAME);
    }).then((subtask)=> {
      expect(subtask.transfer.documents.index).to.eql('myindex1');
      expect(subtask.transfer.documents.type).to.eql('mytype1');
      return manager.fetchSubtask(TASK_NAME);
    }).then((subtask)=> {
      expect(subtask.transfer.documents.index).to.eql('myindex1');
      expect(subtask.transfer.documents.type).to.eql('mytype2');
      return manager.fetchSubtask(TASK_NAME);
    }).then((subtask)=> {
      expect(subtask).to.be.null;
      done();
    }).catch(done);
  });


  it('should get subtasks in the same order they were added', (done)=> {
    const subtasks = [
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count:       10
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex2',
            type:  'mytype1'
          }
        },
        count:       20
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype4'
          }
        },
        count:       1
      }
    ];

    Promise.each(subtasks, subtask => manager.queueSubtask(TASK_NAME, subtask)).then(()=> {
      return manager.fetchSubtask(TASK_NAME);
    }).then((subtask)=> {
      expect(subtask.transfer.documents.index).to.eql(subtasks[0].transfer.documents.index);
      expect(subtask.transfer.documents.type).to.eql(subtasks[0].transfer.documents.type);
      expect(subtask.count).to.eql(subtasks[0].count);
      return manager.fetchSubtask(TASK_NAME);
    }).then((subtask)=> {
      expect(subtask.transfer.documents.index).to.eql(subtasks[1].transfer.documents.index);
      expect(subtask.transfer.documents.type).to.eql(subtasks[1].transfer.documents.type);
      expect(subtask.count).to.eql(subtasks[1].count);
      return manager.fetchSubtask(TASK_NAME);
    }).then((subtask)=> {
      expect(subtask.transfer.documents.index).to.eql(subtasks[2].transfer.documents.index);
      expect(subtask.transfer.documents.type).to.eql(subtasks[2].transfer.documents.type);
      expect(subtask.count).to.eql(subtasks[2].count);
      return manager.fetchSubtask(TASK_NAME);
    }).then((subtask)=> {
      expect(subtask).to.be.null;
      done();
    }).catch(done);
  });

  it('should not add the same subtask twice', (done)=> {
    const subtasks = [
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count:       22
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count:       22
      }
    ];

    Promise.each(subtasks, subtask => manager.queueSubtask(TASK_NAME, subtask)).then(()=> {
      return manager.fetchSubtask(TASK_NAME);
    }).then((subtask)=> {
      expect(subtask.transfer.documents.index).to.eql(subtasks[0].transfer.documents.index);
      expect(subtask.transfer.documents.type).to.eql(subtasks[0].transfer.documents.type);
      expect(subtask.count).to.eql(subtasks[0].count);
      return manager.fetchSubtask(TASK_NAME);
    }).then((subtask)=> {
      expect(subtask).to.be.null;
      done();
    }).catch(done);
  });

  it('should get all completed subtasks', (done)=> {
    const subtasks = [
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count:       10
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex2',
            type:  'mytype1'
          }
        },
        count:       20
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype4'
          }
        },
        count:       1
      }
    ];

    Promise.each(subtasks, subtask => manager.completeSubtask(TASK_NAME, subtask)).then(()=> {
      return manager.getCompletedSubtasks(TASK_NAME);
    }).then((completedSubtasks)=> {

      let target = _.find(completedSubtasks, {count: subtasks[0].count});
      expect(target.transfer.documents.index).to.eql(subtasks[0].transfer.documents.index);
      expect(target.transfer.documents.type).to.eql(subtasks[0].transfer.documents.type);
      expect(target.count).to.eql(subtasks[0].count);

      target = _.find(completedSubtasks, {count: subtasks[1].count});
      expect(target.transfer.documents.index).to.eql(subtasks[1].transfer.documents.index);
      expect(target.transfer.documents.type).to.eql(subtasks[1].transfer.documents.type);
      expect(target.count).to.eql(subtasks[1].count);

      target = _.find(completedSubtasks, {count: subtasks[2].count});
      expect(target.transfer.documents.index).to.eql(subtasks[2].transfer.documents.index);
      expect(target.transfer.documents.type).to.eql(subtasks[2].transfer.documents.type);
      expect(target.count).to.eql(subtasks[2].count);

      done();
    }).catch(done);
  });

  it('should get completed subtask count', (done)=> {
    const subtasks = [
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count:       10
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex2',
            type:  'mytype1'
          }
        },
        count:       20
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype4'
          }
        },
        count:       1
      }
    ];

    Promise.each(subtasks, subtask => manager.completeSubtask(TASK_NAME, subtask)).then(()=> {
      return manager.getCompletedCount(TASK_NAME);
    }).then((completedCount)=> {
      expect(completedCount).to.eql(31);
      done();
    }).catch(done);
  });

  it('should clear completed subtasks', (done)=> {
    const subtasks = [
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count:       10
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex2',
            type:  'mytype1'
          }
        },
        count:       20
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype4'
          }
        },
        count:       1
      }
    ];

    Promise.each(subtasks, subtask => manager.completeSubtask(TASK_NAME, subtask)).then(()=> {
      return manager.clearCompletedSubtasks(TASK_NAME);
    }).then(()=> {
      return manager.getCompletedCount(TASK_NAME);
    }).then((completedCount)=> {
      expect(completedCount).to.eql(0);
      done();
    }).catch(done);
  });

  it('should return an empty array when there are no completed subtasks', (done)=> {
    manager.getCompletedSubtasks(TASK_NAME).then((subtasks)=> {
      expect(subtasks).to.eql([]);
      done();
    }).catch(done);
  });

  it('should return an empty array when there are no backlog subtasks', (done)=> {
    manager.getBacklogSubtasks(TASK_NAME).then((subtasks)=> {
      expect(subtasks).to.eql([]);
      done();
    }).catch(done);
  });

  it('should return all backlog subtasks', (done)=> {
    const subtasks = [
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count:       10
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex2',
            type:  'mytype1'
          }
        },
        count:       20
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype4'
          }
        },
        count:       1
      }
    ];

    Promise.each(subtasks, substask => manager.queueSubtask(TASK_NAME, substask)).then(()=> {
      return manager.getBacklogSubtasks(TASK_NAME);
    }).then((backlogSubtasks)=> {

      let target = _.find(backlogSubtasks, {count: subtasks[0].count});
      expect(target.transfer.documents.index).to.eql(subtasks[0].transfer.documents.index);
      expect(target.transfer.documents.type).to.eql(subtasks[0].transfer.documents.type);
      expect(target.count).to.eql(subtasks[0].count);

      target = _.find(backlogSubtasks, {count: subtasks[1].count});
      expect(target.transfer.documents.index).to.eql(subtasks[1].transfer.documents.index);
      expect(target.transfer.documents.type).to.eql(subtasks[1].transfer.documents.type);
      expect(target.count).to.eql(subtasks[1].count);

      target = _.find(backlogSubtasks, {count: subtasks[2].count});
      expect(target.transfer.documents.index).to.eql(subtasks[2].transfer.documents.index);
      expect(target.transfer.documents.type).to.eql(subtasks[2].transfer.documents.type);
      expect(target.count).to.eql(subtasks[2].count);
      done();
    }).catch(done);
  });

  it('should clear all backlog subtasks', (done)=> {
    const subtasks = [
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count:       10
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex2',
            type:  'mytype1'
          }
        },
        count:       20
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype4'
          }
        },
        count:       1
      }
    ];

    Promise.each(subtasks, subtask => manager.queueSubtask(TASK_NAME, subtask)).then(()=> {
      return manager.clearBacklogSubtasks(TASK_NAME);
    }).then(()=> {
      return manager.getBacklogSubtasks(TASK_NAME);
    }).then((backlogSubtasks)=> {
      expect(backlogSubtasks).to.eql([]);
      done();
    }).catch(done);
  });

  it('should return count of zero for empty completed', (done)=> {
    manager.getCompletedCount(TASK_NAME).then((count)=> {
      expect(count).to.eql(0);
      done();
    }).catch(done);
  });

  it('should return count of zero for empty backlog', (done)=> {
    manager.getBacklogCount(TASK_NAME).then((count)=> {
      expect(count).to.eql(0);
      done();
    }).catch(done);
  });

  it('should return total count of subtasks in backlog', (done)=> {
    const subtasks = [
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        },
        count:       10
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex2',
            type:  'mytype1'
          }
        },
        count:       20
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype4'
          }
        },
        count:       1
      }
    ];

    Promise.each(subtasks, subtask => manager.queueSubtask(TASK_NAME, subtask)).then(()=> {
      return manager.getBacklogCount(TASK_NAME);
    }).then((backlogTotal)=> {
      const subtaskTotal = _.reduce(subtasks, (total, subtask)=> {
        total += subtask.count;
        return total;
      }, 0);

      expect(backlogTotal).to.eql(subtaskTotal);
      done();
    }).catch(done);
  });

  it('should get counts for provided jobs', (done)=> {
    const subtasks = [
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex1',
            type:  'mytype1'
          }
        }
      },
      {
        source:      {
          host:       SOURCE_ES_HOST,
          apiVersion: '1.4'
        },
        destination: {
          host:       DEST_ES_HOST,
          apiVersion: '2.2'
        },
        transfer:    {
          documents: {
            index: 'myindex3',
            type:  'mytype3'
          }
        }
      }
    ];

    addData(source).then(()=> {
      return manager._addCountToSubtasks(source, subtasks);
    }).then((subtasksWithCount)=> {
      expect(subtasksWithCount.length).to.eql(2);

      let filter = {transfer: {documents: {index: 'myindex1'}}};
      let target = _.find(subtasksWithCount, filter);
      expect(target.count).to.eql(1);

      filter = {transfer: {documents: {index: 'myindex3'}}};
      target = _.find(subtasksWithCount, filter);
      expect(target.count).to.eql(2);

      done();
    }).catch(done);
  });

  it('should add task and create subtasks in backlog', done => {
    const task = {
      source:      {
        host:       SOURCE_ES_HOST,
        apiVersion: '1.4'
      },
      destination: {
        host:       DEST_ES_HOST,
        apiVersion: '2.2'
      },
      transfer:        {
        documents: {
          fromIndices: '*'
        }
      }
    };

    addData(source).then(()=> {
      return manager.addTask(TASK_NAME, task);
    }).then(()=> {
      return manager.getBacklogSubtasks(TASK_NAME);
    }).then((subtasks)=> {
      expect(subtasks.length).to.eql(5);
      return manager.getTasks();
    }).then((tasks)=> {
      expect(tasks.length).to.eql(1);
      expect(tasks[0]).to.eql(TASK_NAME);
      done();
    }).catch(done);
  });

  it('should return list of tasks', done => {
    const task = {
      source:      {
        host:       SOURCE_ES_HOST,
        apiVersion: '1.4'
      },
      destination: {
        host:       DEST_ES_HOST,
        apiVersion: '2.2'
      },
      transfer:        {
        documents: {
          fromIndices: '*'
        }
      }
    };

    manager.addTask(TASK_NAME, task).then(()=> {
      return manager.getTasks();
    }).then(taskNames => {
      expect(taskNames).to.eql([TASK_NAME]);
      done();
    }).catch(done);
  });

  it('should return empty list when there are no tasks', done => {
    manager.getTasks().then(taskNames => {
      expect(taskNames).to.eql([]);
      done();
    });
  });

  it('should log and return errors', (done)=> {
    const subtask = {
      source:      {
        host:       SOURCE_ES_HOST,
        apiVersion: '1.4'
      },
      destination: {
        host:       DEST_ES_HOST,
        apiVersion: '2.2'
      },
      transfer:    {
        documents: {
          index: 'myindex1',
          type:  'mytype1'
        }
      },
      count:       10
    };

    manager.logError(TASK_NAME, subtask, 'something broke').delay(5).then(()=> {
      return manager.logError(TASK_NAME, subtask, 'something else broke');
    }).then(()=> {
      return manager.getErrors(TASK_NAME);
    }).then((errors)=> {
      expect(errors.length).to.eql(2);
      expect(errors[0].subtask).to.be.instanceOf(Subtask);
      expect(errors[0].subtask.source).to.eql(subtask.source);
      expect(errors[0].subtask.destination).to.eql(subtask.destination);
      expect(errors[0].subtask.transfer).to.eql(subtask.transfer);
      expect(errors[0].subtask.count).to.eql(subtask.count);
      expect(errors[0].message).to.eql('something broke');

      expect(errors[1].subtask).to.be.instanceOf(Subtask);
      expect(errors[1].subtask.source).to.eql(subtask.source);
      expect(errors[1].subtask.destination).to.eql(subtask.destination);
      expect(errors[1].subtask.transfer).to.eql(subtask.transfer);
      expect(errors[1].subtask.count).to.eql(subtask.count);
      expect(errors[1].message).to.eql('something else broke');
      done();
    }).catch(done);
  });

  it('should keep track of progress for a single task', (done)=> {
    const subtask = {
      source:      {
        host:       SOURCE_ES_HOST,
        apiVersion: '1.4'
      },
      destination: {
        host:       DEST_ES_HOST,
        apiVersion: '2.2'
      },
      transfer:    {
        documents: {
          index: 'myindex1',
          type:  'mytype1'
        }
      },
      count:       10
    };

    const progressUpdate = {
      tick:        10,
      total:       20,
      transferred: 10
    };

    manager.updateProgress(TASK_NAME, subtask, progressUpdate).then(()=> {
      return manager.getProgress(TASK_NAME, subtask);
    }).then((progress)=> {
      expect(progress.tick).to.eql(10);
      expect(progress.total).to.eql(20);
      expect(progress.transferred).to.eql(10);
      expect(progress.lastModified).to.not.be.undefined;

      progressUpdate.tick        = 5;
      progressUpdate.transferred = 15;

      return manager.updateProgress(TASK_NAME, subtask, progressUpdate);
    }).then(()=> {
      return manager.getProgress(TASK_NAME, subtask);
    }).then((progress)=> {
      expect(progress.tick).to.eql(5);
      expect(progress.total).to.eql(20);
      expect(progress.transferred).to.eql(15);
      expect(progress.lastModified).to.not.be.undefined;
      done();
    }).catch(done);
  });

  it('should keep track of progress for multiple tasks', (done)=> {
    const subtask1 = {
      source:      {
        host:       SOURCE_ES_HOST,
        apiVersion: '1.4'
      },
      destination: {
        host:       DEST_ES_HOST,
        apiVersion: '2.2'
      },
      transfer:    {
        documents: {
          index: 'myindex1',
          type:  'mytype1'
        }
      },
      count:       10
    };

    const subtask2 = {
      source:      {
        host:       SOURCE_ES_HOST,
        apiVersion: '1.4'
      },
      destination: {
        host:       DEST_ES_HOST,
        apiVersion: '2.2'
      },
      transfer:    {
        documents: {
          index: 'myindex3',
          type:  'mytype1'
        }
      },
      count:       25
    };

    const progressUpdate = {
      tick:        10,
      total:       20,
      transferred: 10
    };

    manager.updateProgress(TASK_NAME, subtask1, progressUpdate).then(()=> {
      return manager.getProgress(TASK_NAME, subtask1);
    }).then((progress)=> {
      expect(progress.tick).to.eql(10);
      expect(progress.total).to.eql(20);
      expect(progress.transferred).to.eql(10);
      expect(progress.lastModified).to.not.be.undefined;

      progressUpdate.tick        = 5;
      progressUpdate.transferred = 15;

      return manager.updateProgress(TASK_NAME, subtask2, progressUpdate);
    }).then(()=> {
      return manager.getProgress(TASK_NAME, subtask2);
    }).then((progress)=> {
      expect(progress.tick).to.eql(5);
      expect(progress.total).to.eql(20);
      expect(progress.transferred).to.eql(15);
      expect(progress.lastModified).to.not.be.undefined;
      return manager.getOverallProgress(TASK_NAME);
    }).then((overallProgress)=> {
      expect(overallProgress.length).to.eql(2);

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
      expect(target.progress.tick).to.eql(10);

      predicate.subtask.transfer.documents.index = 'myindex3';

      target = _.find(overallProgress, predicate);
      expect(target.progress.tick).to.eql(5);
      done();
    }).catch(done);
  });

  it('should delete progress of specific subtask', (done)=> {
    const subtask1 = {
      source:      {
        host:       SOURCE_ES_HOST,
        apiVersion: '1.4'
      },
      destination: {
        host:       DEST_ES_HOST,
        apiVersion: '2.2'
      },
      transfer:    {
        documents: {
          index: 'myindex1',
          type:  'mytype1'
        }
      },
      count:       10
    };

    const subtask2 = {
      source:      {
        host:       SOURCE_ES_HOST,
        apiVersion: '1.4'
      },
      destination: {
        host:       DEST_ES_HOST,
        apiVersion: '2.2'
      },
      transfer:    {
        documents: {
          index: 'myindex3',
          type:  'mytype1'
        }
      },
      count:       25
    };

    const progressUpdate = {
      tick:        10,
      total:       20,
      transferred: 10
    };

    manager.updateProgress(TASK_NAME, subtask1, progressUpdate).then(()=> {
      return manager.getProgress(TASK_NAME, subtask1);
    }).then(()=> {
      progressUpdate.tick        = 5;
      progressUpdate.transferred = 15;

      return manager.updateProgress(TASK_NAME, subtask2, progressUpdate);
    }).then(()=> {
      return manager.getProgress(TASK_NAME, subtask2);
    }).then(()=> {
      return manager.getOverallProgress(TASK_NAME);
    }).then((overallProgress)=> {
      expect(overallProgress.length).to.eql(2);
      return manager.removeProgress(TASK_NAME, subtask1);
    }).then(()=> {
      return manager.getOverallProgress(TASK_NAME);
    }).then((overallProgress)=> {
      expect(overallProgress.length).to.eql(1);
      expect(overallProgress[0].subtask.transfer.documents.index).to.eql('myindex3');
      expect(overallProgress[0].progress.tick).to.eql(5);
      done();
    }).catch(done);
  });

  it('should not accept the same name twice', done => {
    const names = [
      'same',
      'same',
      'different'
    ];

    const getName = () => {
      return names.shift();
    };

    manager._setWorkerName(getName).then(name => {
      expect(name).to.eql('same');
      return manager._setWorkerName(getName);
    }).then(name => {
      expect(name).to.eql('different');
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

    manager._setWorkerName(getName).delay(1100).then(name => {
      expect(name).to.eql('same');
      return manager._setWorkerName(getName);
    }).then(name => {
      expect(name).to.eql('same');
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

    manager._setWorkerName(getName).then(name => {
      expect(name).to.eql('same');
      return manager._setWorkerName(getName);
    }).then(name => {
      expect(name).to.eql('different');
      return manager.workerHeartbeat('same', 'running');
    }).then(() => {
      return manager.workerHeartbeat('different', 'broken');
    }).then(() => {
      return manager.getWorkersStatus();
    }).delay(500).then(status => {
      expect(_.size(status)).to.eql(2);
      expect(status.same).to.eql('running');
      expect(status.different).to.eql('broken');
      return manager.workerHeartbeat('same', 'running');
    }).delay(600).then(()=> {
      return manager.getWorkersStatus();
    }).then(status => {
      expect(_.size(status)).to.eql(1);
      expect(status.same).to.eql('running');
      done();
    }).catch(done);
  });

  afterEach((done)=> {
    source.indices.deleteTemplate({name: '*'}).finally(()=> {
      return source.indices.delete({index: '*'});
    }).finally(()=> {
      return redis.flushdb();
    }).finally(()=> {
      done();
    });
  });
});
