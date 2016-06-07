const expect            = require('chai').expect;
const Worker            = require('../../app/services/worker');
const createEsClient    = require('../../config/elasticsearch.js');
const createRedisClient = require('../../config/redis');
const config            = require('../../config/index');
const Manager           = require('../../app/services/manager');
const _                 = require('lodash');

const log = config.log;

const Promise = require('bluebird');
Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection((error) => {
  log.error('Likely error: ', error.stack);
});

describe('worker', function () {
  this.timeout(10000);

  const sourceConfig = {
    host:       'localhost:9200',
    apiVersion: '1.4'
  };

  const destConfig = {
    host:       'localhost:9201',
    apiVersion: '2.2'
  };

  const TASK1_NAME = 'task1';
  const source     = createEsClient(sourceConfig.host, sourceConfig.apiVersion);
  const dest       = createEsClient(destConfig.host, destConfig.apiVersion);
  const redis      = createRedisClient('localhost', 6379);

  before((done)=> {
    Worker.__overrideCheckInterval(100);

    source.indices.deleteTemplate({name: '*'}).finally(()=> {
      return dest.indices.deleteTemplate({name: '*'});
    }).finally(()=> {
      return source.indices.delete({index: '*'});
    }).finally(()=> {
      return dest.indices.delete({index: '*'});
    }).finally(()=> {
      return redis.flushdb();
    }).finally(()=> {
      done();
    });
  });

  it('should perform transfers queued by manager', (done)=> {
    const taskParams = {
      source:      sourceConfig,
      destination: destConfig,
      transfer:        {
        documents: {
          fromIndices: '*'
        }
      }
    };

    const indexConfigs = [
      {
        index: 'first',
        type:  'type1'
      },
      {
        index: 'first',
        type:  'type2'
      },
      {
        index: 'second',
        type:  'mytype1'
      }
    ];

    const data = [];

    _.times(10, (n)=> {
      data.push({
        index: {
          _index: indexConfigs[0].index,
          _type:  indexConfigs[0].type
        }
      });
      data.push({something: `data${n}`});
    });

    _.times(15, (n)=> {
      data.push({
        index: {
          _index: indexConfigs[1].index,
          _type:  indexConfigs[1].type
        }
      });
      data.push({something: `data${n}`});
    });

    _.times(5, (n)=> {
      data.push({
        index: {
          _index: indexConfigs[2].index,
          _type:  indexConfigs[2].type
        }
      });
      data.push({something: `data${n}`});
    });

    let totalTransferred  = 0;
    const progressUpdates = (taskName, subtask, update)=> {
      // log.info('update', update);
      totalTransferred += update.tick;
    };

    const completedSubtasks = [];
    const completeCallback  = (taskName, subtask)=> {
      expect(taskName).to.eql(TASK1_NAME);
      completedSubtasks.push(subtask);

      if (completedSubtasks.length >= 3) {
        expect(totalTransferred).to.eql(30);
        manager.setRunning(false);
        worker.killStopped();
        done();
      }
    };

    const manager = new Manager(redis);
    const worker  = new Worker(redis);
    worker.setUpdateCallback(progressUpdates);
    worker.setCompletdCallback(completeCallback);

    source.bulk({body: data}).then((results)=> {
      if (results.errors) {
        log.error('errors', JSON.stringify(results, null, 2));
        done('fail');
        return Promise.reject(`errors: ${results.errors}`);
      }

      return source.indices.refresh();
    }).then(()=> {
      return dest.indices.delete({
        index:   '*',
        refresh: true
      });
    }).then(()=> {
      return dest.search('*');
    }).then((results)=> {
      expect(results.hits.total).to.eql(0);
      return manager.addTask(TASK1_NAME, taskParams);
    }).then(()=> {
      return manager.getBacklogSubtasks(TASK1_NAME);
    }).then((backlogJobs)=> {
      expect(backlogJobs.length).to.eql(3);

      let target = _.find(backlogJobs, {
        transfer: {
          documents: {
            index: 'first',
            type:  'type1'
          }
        }
      });
      expect(target.count).to.eql(10);

      target = _.find(backlogJobs, {
        transfer: {
          documents: {
            index: 'first',
            type:  'type2'
          }
        }
      });
      expect(target.count).to.eql(15);

      target = _.find(backlogJobs, {
        transfer: {
          documents: {
            index: 'second',
            type:  'mytype1'
          }
        }
      });
      expect(target.count).to.eql(5);

      return manager.setRunning(true);
    });
  });
});