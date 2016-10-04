const chai       = require('chai');
const expect     = chai.expect;
const asPromised = require('chai-as-promised');
chai.use(asPromised);
const TestConfig   = require('../config');
const Task         = require('../../app/models/task');
const TasksService = require('../../app/services/tasks');

describe('tasks service', function () {
  this.timeout(5000);

  it('does not add a task if a mutator cannot be found in redis', (done) => {

    const task = new Task({
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      transfer:    {
        documents: {
          fromIndices: '*'
        }
      },
      mutators: {
        actions: [{id: 'doesNotExist'}]
      }
    });

    const redisClient = {
      sismember: () => Promise.resolve(false),
      sadd:      () => done(new Error('Should not be called.')),
      del:       () => done(new Error('Should not be called.')),
      hget:      () => Promise.resolve(null)
    };

    const tasksService = new TasksService(redisClient);
    expect(tasksService.add('testid', task)).to.be.rejectedWith('Src for mutator id doesNotExist not found')
    .then(() => done());
  });

  it('does not add a task if a filter cannot be found in redis', (done) => {

    const task = new Task({
      source:      TestConfig.elasticsearch.source,
      destination: TestConfig.elasticsearch.destination,
      transfer:    {
        documents: {
          fromIndices: '*',
          filters:     {
            actions: [{id: 'doesNotExist'}]
          }
        }
      }
    });

    const redisClient = {
      sismember: () => Promise.resolve(false),
      sadd:      () => done(new Error('Should not be called.')),
      del:       () => done(new Error('Should not be called.')),
      hexists:   () => Promise.resolve(false)
    };

    const tasksService = new TasksService(redisClient);
    expect(tasksService.add('testid', task)).to.be.rejectedWith('Src for filter id doesNotExist not found')
    .then(() => done());
  });

});