import { expect } from 'chai';
import Manager from '../lib/manager';
import config from '../config';
import createEsClient from '../config/elasticsearch.js'
import redis from '../config/redis';
import _ from 'lodash';

var log = config.log;

import Promise from 'bluebird';
Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection(function (error) {
  log.error('Likely error: ', error.stack);
});

describe('job manager', function () {
  // this.timeout(8000);

  var manager = null;
  var source  = null;

  before((done)=> {
    source = createEsClient('localhost:9200', '1.4');

    manager = new Manager(source);

    source.indices.deleteTemplate({name: '*'}).finally(()=> {
      return source.indices.delete({index: '*'});
    }).finally(()=> {
      return redis.flushdb();
    }).finally(()=> { done(); });
  });

  var allIndices = [
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

  it('should get all indices and types', (done)=> {
    addData(manager.source).then(()=> {
      return manager.getIndices(manager.source, '*');
    }).then((indices)=> {
      expect(_.size(indices)).to.eql(3);

      let myindex1 = _.find(indices, {name: 'myindex1'});
      expect(myindex1).to.be.defined;
      expect(_.size(myindex1.mappings)).to.eql(2);
      expect(myindex1.mappings.mytype1).to.be.defined;
      expect(myindex1.mappings.mytype2).to.be.defined;

      let myindex2 = _.find(indices, {name: 'myindex2'});
      expect(myindex2).to.be.defined;
      expect(_.size(myindex2.mappings)).to.eql(1);
      expect(myindex2.mappings.mytype1).to.be.defined;

      let myindex3 = _.find(indices, {name: 'myindex3'});
      expect(myindex3).to.be.defined;
      expect(_.size(myindex1.mappings)).to.eql(2);
      expect(myindex3.mappings.mytype2).to.be.defined;
      expect(myindex3.mappings.mytype3).to.be.defined;

      done();
    });
  });

  it('filter out indices by regex', ()=> {
    manager.setIndexFilter(/index_number_1/);

    let filtered = manager.filterIndicesAndTypes(allIndices);

    expect(_.size(filtered)).to.eql(1);
    expect(filtered[0].index).to.eql('index_number_1');
    expect(_.size(filtered[0].types)).to.eql(3);
  });

  it('filter out indices by function', ()=> {
    manager.setIndexFilter((index)=> {
      return index.name === 'index_number_2';
    });

    let filtered = manager.filterIndicesAndTypes(allIndices);

    expect(_.size(filtered)).to.eql(1);
    expect(filtered[0].index).to.eql('index_number_2');
    expect(_.size(filtered[0].types)).to.eql(2);
  });

  it('filter out types by regex', ()=> {
    manager.setTypeFilter(/^newtype$/);

    let filtered = manager.filterIndicesAndTypes(allIndices);

    expect(_.size(filtered)).to.eql(2);

    let target = _.find(filtered, {index: 'index_number_1'});
    expect(_.size(target.types)).to.eql(1);
    expect(target.types).to.include('newtype');

    target = _.find(filtered, {index: 'index_number_2'});
    expect(_.size(target.types)).to.eql(1);
    expect(target.types).to.include('newtype');
  });

  it('filter out types by function', ()=> {
    manager.setTypeFilter((type)=> {
      return type.name === 'newtype_3';
    });

    let filtered = manager.filterIndicesAndTypes(allIndices);

    expect(_.size(filtered)).to.eql(1);

    expect(_.size(filtered[0].types)).to.eql(1);
    expect(filtered[0].types).to.include('newtype_3');
  });

  it('reject index filters that are not functions, functions with incorrect arguments, or regex\'s', ()=> {
    let throws = ()=> {
      manager.setIndexFilter('no_strings');
    };
    expect(throws).to.throw(/filter must be a regex or function that takes 1 argument/);

    throws = ()=> {
      manager.setIndexFilter({stuff: 'something'});
    };
    expect(throws).to.throw(/filter must be a regex or function that takes 1 argument/);

    throws = ()=> {
      manager.setIndexFilter(()=> {});
    };
    expect(throws).to.throw(/filter must be a regex or function that takes 1 argument/);

    throws = (first, second)=> {
      manager.setIndexFilter(()=> {});
    };
    expect(throws).to.throw(/filter must be a regex or function that takes 1 argument/);
  });

  it('reject type filters that are not functions, functions with incorrect arguments, or regex\'s', ()=> {
    let throws = ()=> {
      manager.setTypeFilter('no_strings');
    };
    expect(throws).to.throw(/filter must be a regex or function that takes 1 argument/);

    throws = ()=> {
      manager.setTypeFilter({stuff: 'something'});
    };
    expect(throws).to.throw(/filter must be a regex or function that takes 1 argument/);

    throws = ()=> {
      manager.setTypeFilter(()=> {});
    };
    expect(throws).to.throw(/filter must be a regex or function that takes 1 argument/);

    throws = (first, second)=> {
      manager.setTypeFilter(()=> {});
    };
    expect(throws).to.throw(/filter must be a regex or function that takes 1 argument/);
  });

  it('should sort indices depending on comparator', (done)=> {
    manager.setIndexComparator((a, b)=> {
      return a.localeCompare(b);
    });

    addData(source).then(()=> {
      return manager.prepareNewJobs('*');
    }).then((jobs)=> {
      expect(_.size(jobs)).to.eql(5);

      // Cannot necessarily be sure of the type order, so that is not checked
      expect(jobs[0].index).to.eql('myindex1');
      expect(jobs[1].index).to.eql('myindex1');
      expect(jobs[2].index).to.eql('myindex2');
      expect(jobs[3].index).to.eql('myindex3');
      expect(jobs[4].index).to.eql('myindex3');

      manager.setIndexComparator((a, b)=> {
        return b.localeCompare(a);
      });

      return manager.prepareNewJobs('*');
    }).then((jobs)=> {
      expect(_.size(jobs)).to.eql(5);
      expect(jobs[0].index).to.eql('myindex3');
      expect(jobs[1].index).to.eql('myindex3');
      expect(jobs[2].index).to.eql('myindex2');
      expect(jobs[3].index).to.eql('myindex1');
      expect(jobs[4].index).to.eql('myindex1');

      done();
    });
  });

  it('should prep job backlog', (done)=> {
    manager.setIndexComparator((a, b)=> {
      return a.localeCompare(b);
    });

    var completedJob = {
      index: 'myindex2',
      type:  'mytype1',
      count: 10
    };

    manager.completeJob(completedJob).then(()=> {
      return addData(source);
    }).then(()=> {
      return manager.initialize('*');
    }).then(()=> {
      return manager.fetchJob();
    }).then((job)=> {
      expect(job.index).to.eql('myindex1');
      return manager.fetchJob();
    }).then((job)=> {
      expect(job.index).to.eql('myindex1');
      return manager.fetchJob();
    }).then((job)=> {
      expect(job.index).to.eql('myindex3');
      return manager.fetchJob();
    }).then((job)=> {
      expect(job.index).to.eql('myindex3');
      return manager.fetchJob();
    }).then((job)=> {
      expect(job).to.be.null;
      done();
    });
  });


  it('get jobs in the same order they were added', (done)=> {
    var jobs = [
      {
        index: 'index1',
        type:  'type1',
        count: 22
      },
      {
        index: 'index2',
        type:  'type1',
        count: 15
      },
      {
        index: 'index1',
        type:  'type2',
        count: 9
      },
      {
        index: 'index4',
        type:  'type1',
        count: 120
      }
    ];

    Promise.each(jobs, manager.queueJob).then(()=> {
      return manager.fetchJob();
    }).then((job)=> {
      expect(job.index).to.eql(jobs[0].index);
      expect(job.type).to.eql(jobs[0].type);
      expect(job.count).to.eql(jobs[0].count);
      return manager.fetchJob();
    }).then((job)=> {
      expect(job.index).to.eql(jobs[1].index);
      expect(job.type).to.eql(jobs[1].type);
      expect(job.count).to.eql(jobs[1].count);
      return manager.fetchJob();
    }).then((job)=> {
      expect(job.index).to.eql(jobs[2].index);
      expect(job.type).to.eql(jobs[2].type);
      expect(job.count).to.eql(jobs[2].count);
      return manager.fetchJob();
    }).then((job)=> {
      expect(job.index).to.eql(jobs[3].index);
      expect(job.type).to.eql(jobs[3].type);
      expect(job.count).to.eql(jobs[3].count);
      return manager.fetchJob();
    }).then((job)=> {
      expect(job).to.be.null;
      done();
    });
  });

  it('should throw if invalid job is added', (done)=> {

    let throws = ()=> {
      manager.queueJob({
        index: 'missing_count',
        type:  'type1'
      })
    };

    expect(throws).to.throw(/count must be number gte 0/);

    throws = ()=> {
      manager.queueJob({
        index: 'indexname',
        type:  'type1',
        count: 'not a number'
      })
    };

    expect(throws).to.throw(/count must be number gte 0/);

    throws = ()=> {
      manager.queueJob({
        index: 'indexname',
        type:  'type1',
        count: -10
      })
    };

    expect(throws).to.throw(/count must be number gte 0/);

    throws = ()=> {
      manager.queueJob({
        type:  'type1',
        count: 10
      })
    };

    expect(throws).to.throw(/index must be string with length/);

    throws = ()=> {
      manager.queueJob({
        index: 10,
        type:  'type1',
        count: 10
      })
    };

    expect(throws).to.throw(/index must be string with length/);

    throws = ()=> {
      manager.queueJob({
        index: 'indename',
        count: 10
      })
    };

    expect(throws).to.throw(/type must be string with length/);

    throws = ()=> {
      manager.queueJob({
        index: 'indexname',
        type:  10,
        count: 10
      })
    };

    expect(throws).to.throw(/type must be string with length/);
    done();
  });

  it('should get all completed jobs', (done)=> {
    var jobs = [
      {
        index: 'index1',
        type:  'type1',
        count: 22
      },
      {
        index: 'index2',
        type:  'type1',
        count: 15
      },
      {
        index: 'index1',
        type:  'type2',
        count: 9
      },
      {
        index: 'index4',
        type:  'type1',
        count: 120
      }
    ];

    Promise.each(jobs, manager.completeJob).then(()=> {
      return manager.getCompletedJobs();
    }).then((completedJobs)=> {

      let target = _.find(completedJobs, {count: jobs[0].count});
      expect(target.index).to.eql(jobs[0].index);
      expect(target.type).to.eql(jobs[0].type);
      expect(target.count).to.eql(jobs[0].count);

      target = _.find(completedJobs, {count: jobs[1].count});
      expect(target.index).to.eql(jobs[1].index);
      expect(target.type).to.eql(jobs[1].type);
      expect(target.count).to.eql(jobs[1].count);

      target = _.find(completedJobs, {count: jobs[2].count});
      expect(target.index).to.eql(jobs[2].index);
      expect(target.type).to.eql(jobs[2].type);
      expect(target.count).to.eql(jobs[2].count);

      target = _.find(completedJobs, {count: jobs[3].count});
      expect(target.index).to.eql(jobs[3].index);
      expect(target.type).to.eql(jobs[3].type);
      expect(target.count).to.eql(jobs[3].count);
      done();
    });
  });

  it('should return an empty array when there are no completed jobs', (done)=> {
    manager.getCompletedJobs().then((jobs)=> {
      expect(jobs).to.eql([]);
      done();
    });
  });

  it('should return all backlog jobs', (done)=> {
    var jobs = [
      {
        index: 'index1',
        type:  'type1',
        count: 22
      },
      {
        index: 'index2',
        type:  'type1',
        count: 15
      },
      {
        index: 'index1',
        type:  'type2',
        count: 9
      },
      {
        index: 'index4',
        type:  'type1',
        count: 120
      }
    ];

    Promise.each(jobs, manager.queueJob).then(()=> {
      return manager.getBacklogJobs();
    }).then((backlogJobs)=> {

      let target = _.find(backlogJobs, {count: jobs[0].count});
      expect(target.index).to.eql(jobs[0].index);
      expect(target.type).to.eql(jobs[0].type);
      expect(target.count).to.eql(jobs[0].count);

      target = _.find(backlogJobs, {count: jobs[1].count});
      expect(target.index).to.eql(jobs[1].index);
      expect(target.type).to.eql(jobs[1].type);
      expect(target.count).to.eql(jobs[1].count);

      target = _.find(backlogJobs, {count: jobs[2].count});
      expect(target.index).to.eql(jobs[2].index);
      expect(target.type).to.eql(jobs[2].type);
      expect(target.count).to.eql(jobs[2].count);

      target = _.find(backlogJobs, {count: jobs[3].count});
      expect(target.index).to.eql(jobs[3].index);
      expect(target.type).to.eql(jobs[3].type);
      expect(target.count).to.eql(jobs[3].count);
      done();
    });
  });

  it('should return total count of jobs in backlog', (done)=> {
    var jobs = [
      {
        index: 'index1',
        type:  'type1',
        count: 22
      },
      {
        index: 'index2',
        type:  'type1',
        count: 15
      },
      {
        index: 'index1',
        type:  'type2',
        count: 9
      },
      {
        index: 'index4',
        type:  'type1',
        count: 120
      }
    ];

    Promise.each(jobs, manager.queueJob).then(()=> {
      return manager.getBacklogCount();
    }).then((backlogTotal)=> {
      let jobTotal = _.reduce(jobs, (total, job)=> {
        total += job.count;
        return total;
      }, 0);

      expect(backlogTotal).to.eql(jobTotal);
      done();
    });
  });

  it('should get counts for provided jobs', (done)=> {
    var jobs = [
      {
        index: 'myindex1',
        type:  'mytype1'
      },
      {
        index: 'myindex3',
        type:  'mytype3'
      }
    ];

    addData(source).then(()=> {
      return manager._addCountToJobs(jobs);
    }).then((jobsWithCount)=> {
      expect(jobsWithCount.length).to.eql(2);

      let target = _.find(jobsWithCount, {index: 'myindex1'});
      expect(target.count).to.eql(1);

      target = _.find(jobsWithCount, {index: 'myindex3'});
      expect(target.count).to.eql(2);

      done();
    });
  });

  afterEach((done)=> {
    manager._resetFiltersAndComparators();

    source.indices.deleteTemplate({name: '*'}).finally(()=> {
      return source.indices.delete({index: '*'});
    }).finally(()=> {
      return redis.flushdb();
    }).finally(()=> { done(); });
  });
});