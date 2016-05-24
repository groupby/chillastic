const expect         = require('chai').expect;
const Worker         = require('../app/worker');
const createEsClient = require('../config/elasticsearch.js');
const config         = require('../config');
const redis          = require('../config/redis');
const Manager        = require('../app/manager');
const _              = require('lodash');

const log = config.log;

const Promise = require('bluebird');
Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection((error) => {
  log.error('Likely error: ', error.stack);
});

describe('worker', function () {
  this.timeout(6000);

  let worker  = null;
  let manager = null;
  let source  = null;
  let dest    = null;

  before((done)=> {
    source = createEsClient('localhost:9200', '1.4');
    dest   = createEsClient('localhost:9201', '2.2');

    worker  = new Worker('localhost:9200', 'localhost:9201');
    manager = new Manager(source);

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

  it('should transfer all data ', (done)=> {
    const jobs = [
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

    _.times(100, (n)=> {
      data.push({
        index: {
          _index: jobs[0].index,
          _type:  jobs[0].type
        }
      });
      data.push({something: `data${n}`});
    });

    _.times(80, (n)=> {
      data.push({
        index: {
          _index: jobs[1].index,
          _type:  jobs[1].type
        }
      });
      data.push({something: `data${n}`});
    });

    _.times(20, (n)=> {
      data.push({
        index: {
          _index: jobs[2].index,
          _type:  jobs[2].type
        }
      });
      data.push({something: `data${n}`});
    });

    let totalTransferred = 0;
    const progressUpdates  = (update)=> {
      // log.info('update', update);
      totalTransferred += update.tick;
    };

    worker._overrideProgresUpdate(progressUpdates);

    source.bulk({body: data}).then((results)=> {
      if (results.errors) {
        log.error('errors', results);
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
      return manager.initialize('*');
    }).then(()=> {
      return manager.getBacklogJobs();
    }).then((backlogJobs)=> {
      expect(backlogJobs.length).to.eql(3);

      let target = _.find(backlogJobs, {
        index: 'first',
        type:  'type1'
      });
      expect(target.count).to.eql(100);

      target = _.find(backlogJobs, {
        index: 'first',
        type:  'type2'
      });
      expect(target.count).to.eql(80);

      target = _.find(backlogJobs, {
        index: 'second',
        type:  'mytype1'
      });
      expect(target.count).to.eql(20);

      return worker.start();
    }).then(()=> {
      expect(totalTransferred).to.eql(200);
      done();
    });
  });

  afterEach(()=> {
    worker._overrideProgresUpdate(null);
  });

  after((done)=> {
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
});