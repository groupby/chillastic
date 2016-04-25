import { expect } from 'chai';
import Master from '../lib/master';
import config from '../config';
import redis from '../config/redis';
import _ from 'lodash';

var log = config.log;

import Promise from 'bluebird';
Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection(function (error) {
  log.error('Likely error: ', error.stack);
});

describe('cluster master tests', function () {
  this.timeout(8000);

  var master = null;

  before(()=> {
    master = new Master('localhost:9200', 'localhost:9201');
  });

  var addData = (client)=> {
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

  // it('should push data into ES', (done)=>{
  //
  // });

  // it('should start a worker, and transfer data', (done)=> {
  //
  //   var params = {
  //     data:        '*',
  //     concurrency: 1
  //   };
  //
  //   var completed = ()=> {
  //     done();
  //   };
  //
  //   master.setCompletedCallback(completed);
  //
  //   addData(master.source).then(()=> {
  //     return master.start(params);
  //   });
  // });


  after((done)=> {
    master.source.indices.deleteTemplate({name: '*'}).finally(()=> {
      return master.dest.indices.deleteTemplate({name: '*'});
    }).finally(()=> {
      return master.source.indices.delete({index: '*'});
    }).finally(()=> {
      return master.dest.indices.delete({index: '*'});
    }).finally(()=> {
      return redis.flushdb();
    }).finally(()=> { done(); });
  });
});