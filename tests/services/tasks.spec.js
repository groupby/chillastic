/*eslint no-magic-numbers: "off"*/
/*eslint no-invalid-this: "off"*/
const _                 = require('lodash');
const expect            = require('chai').expect;
const Promise           = require('bluebird');
const TestConfig        = require('../config');
const Utils             = require('../utils');
const Subtask           = require('../../app/models/subtask');
const Task              = require('../../app/models/task');
const Subtasks          = require('../../app/services/subtasks');
const Tasks             = require('../../app/services/tasks');
const createEsClient    = require('../../config/elasticsearch');
const createRedisClient = require('../../config/redis');
const config            = require('../../config/index');

const log = config.log;

Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection((error) => log.error('Likely error: ', error.stack));

const TASK_NAME = 'testTask';

describe('tasks service', function () {
  this.timeout(5000);

  let source   = null;
  let redis    = null;
  let utils    = null;
  let tasks    = null;
  let subtasks = null;

  before((done)=> {
    source   = createEsClient(TestConfig.elasticsearch.source);
    redis    = createRedisClient(TestConfig.redis.host, TestConfig.redis.port);
    tasks    = new Tasks(redis);
    subtasks = new Subtasks(redis);
    utils    = new Utils();

    source.indices.deleteTemplate({name: '*'})
    .finally(()=> source.indices.delete({index: '*'}))
    .finally(()=> redis.flushdb())
    .finally(()=> done());
  });

  afterEach((done)=> {
    source.indices.deleteTemplate({name: '*'})
    .finally(()=> source.indices.delete({index: '*'}))
    .finally(()=> redis.flushdb())
    .finally(()=> done());
  });

  it('invalid flushSize', (done)=> {
        const task = {
          source:      TestConfig.elasticsearch.source,
          destination: TestConfig.elasticsearch.destination,
          transfer:    {
            flushSize: 10000,
            documents: {
              fromIndices: '*'
            }
          }
        };
        tasks.add(TASK_NAME, task)
        .then(()=> done('fail'))
        .catch((e)=> {
          expect(e.message).equals(`flushSize must be ${Task.DEFAULT_FLUSH_SIZE} or less, given 10000`);
          done();
        })
      }
  );

  it('should add task and create subtasks in backlog', (done)=> {
    const task = {
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      transfer:    {
        documents: {
          fromIndices: '*'
        }
      }
    };

    utils.addData(source)
    .then(()=> tasks.add(TASK_NAME, task))
    .then(()=> subtasks.getBacklog(TASK_NAME))
    .then((backlogSubtasks)=> expect(backlogSubtasks.length).to.be.equals(5))
    .then(()=> tasks.getAll())
    .then((allTasks)=> {
      expect(_.size(allTasks)).to.be.equals(1);
      expect(allTasks[0]).to.be.equals(TASK_NAME);
    })
    .then(()=> done())
    .catch(done);
  });

  it('should return list of tasks', (done)=> {
    const task = {
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      transfer:    {
        documents: {
          fromIndices: '*'
        }
      }
    };

    tasks.add(TASK_NAME, task)
    .then(()=> tasks.getAll())
    .then(taskNames => expect(taskNames).to.eql([TASK_NAME]))
    .then(()=> done())
    .catch(done);
  });

  it('should return empty list when there are no tasks', (done)=> {
    tasks.getAll()
    .then(taskNames => expect(taskNames).to.be.empty)
    .then(()=> done())
    .catch(done);
  });

  it('should log and return errors', (done)=> {
    const subtask = {
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      transfer:    {
        documents: {
          index: 'myindex1',
          type:  'mytype1'
        }
      },
      count:       10
    };

    tasks.logError(TASK_NAME, subtask, 'something broke').delay(5)
    .then(()=> tasks.logError(TASK_NAME, subtask, 'something else broke'))
    .then(()=> tasks.errors(TASK_NAME))
    .then((errors)=> {
      expect(errors.length).to.be.equals(2);
      expect(errors[0].subtask).to.be.an.instanceof(Subtask);
      expect(errors[0].subtask.source).to.eql(subtask.source);
      expect(errors[0].subtask.destination).to.eql(subtask.destination);
      expect(errors[0].subtask.transfer).to.eql(subtask.transfer);
      expect(errors[0].subtask.count).to.be.equals(subtask.count);
      expect(errors[0].message).to.be.equals('something broke');

      expect(errors[1].subtask).to.be.an.instanceof(Subtask);
      expect(errors[1].subtask.source).to.eql(subtask.source);
      expect(errors[1].subtask.destination).to.eql(subtask.destination);
      expect(errors[1].subtask.transfer).to.eql(subtask.transfer);
      expect(errors[1].subtask.count).to.be.equals(subtask.count);
      expect(errors[1].message).to.be.equals('something else broke');
    })
    .then(()=> done())
    .catch(done);
  });

});
