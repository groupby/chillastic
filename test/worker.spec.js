import { expect } from 'chai';
import Worker from '../lib/worker';
import createEsClient from '../config/elasticsearch.js'
import config from '../config';
import redis from '../config/redis';
import Manager from '../lib/manager';
import _ from 'lodash';

var log = config.log;

import Promise from 'bluebird';
Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection(function (error) {
  log.error('Likely error: ', error.stack);
});

describe('worker', function () {
  this.timeout(6000);

  var worker  = null;
  var manager = null;
  var source  = null;
  var dest    = null;

  before((done)=> {
    source = createEsClient('localhost:9200', '1.4');
    dest   = createEsClient('localhost:9201', '2.2');

    worker = new Worker('localhost:9200', 'localhost:9201');
    manager = new Manager(source);

    source.indices.deleteTemplate({name: '*'}).finally(()=> {
      return dest.indices.deleteTemplate({name: '*'});
    }).finally(()=> {
      return source.indices.delete({index: '*'});
    }).finally(()=> {
      return dest.indices.delete({index: '*'});
    }).finally(()=> {
      return redis.flushdb();
    }).finally(()=> { done(); });
  });

  it('should transfer all data ', (done)=> {
    var jobs = [
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

    var data = [];

    _.times(100, (n)=> {
      data.push({
        index: {
          _index: jobs[0].index,
          _type:  jobs[0].type
        }
      });
      data.push({something: 'data' + n});
    });

    _.times(80, (n)=> {
      data.push({
        index: {
          _index: jobs[1].index,
          _type:  jobs[1].type
        }
      });
      data.push({something: 'data' + n});
    });

    _.times(20, (n)=> {
      data.push({
        index: {
          _index: jobs[2].index,
          _type:  jobs[2].type
        }
      });
      data.push({something: 'data' + n});
    });

    var totalTransferred = 0;
    var progressUpdates = (update)=>{
      // log.info('update', update);
      totalTransferred += update.tick;
    };

    worker._overrideProgresUpdate(progressUpdates);

    source.bulk({body: data}).then((results)=> {
      if (results.errors) {
        log.error('errors', results);
        return Promise.reject('errors: ' + results.errors);
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

      let target = _.find(backlogJobs, {index: 'first', type: 'type1'});
      expect(target.count).to.eql(100);

      target = _.find(backlogJobs, {index: 'first', type: 'type2'});
      expect(target.count).to.eql(80);

      target = _.find(backlogJobs, {index: 'second', type: 'mytype1'});
      expect(target.count).to.eql(20);

      return worker.start();
    }).then(()=>{
      expect(totalTransferred).to.eql(200);
      done();
    });
  });

  afterEach(()=>{
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
    }).finally(()=> { done(); });
  });
});