/*eslint no-magic-numbers: "off"*/
const expect         = require('chai').expect;
const Manager        = require('../app/manager');
const config         = require('../config');
const createEsClient = require('../config/elasticsearch.js');
const redis          = require('../config/redis');
const _              = require('lodash');

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

  before((done)=> {
    source = createEsClient('localhost:9200', '1.4');

    manager = new Manager(source);

    source.indices.deleteTemplate({name: '*'}).finally(()=> {
      return source.indices.delete({index: '*'});
    }).finally(()=> {
      return redis.flushdb();
    }).finally(()=> {
      done();
    });
  });

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
    addData(manager.source).then(()=> {
      return manager.source.indices.refresh();
    }).then(()=> {
      return manager.getIndices(manager.source, '*');
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

    const filtered = manager.filterIndicesAndTypes(allIndices);

    expect(_.size(filtered)).to.eql(1);
    expect(filtered[0].index).to.eql('index_number_2');
    expect(_.size(filtered[0].types)).to.eql(2);
  });

  it('filter out types by regex', ()=> {
    manager.setTypeFilter(/^newtype$/);

    const filtered = manager.filterIndicesAndTypes(allIndices);

    expect(_.size(filtered)).to.eql(2);

    let target = _.find(filtered, {index: 'index_number_1'});
    expect(_.size(target.types)).to.eql(1);
    expect(target.types).to.include('newtype');

    target = _.find(filtered, {index: 'index_number_2'});
    expect(_.size(target.types)).to.eql(1);
    expect(target.types).to.include('newtype');
  });

  it('filter out types by regex string', ()=> {
    manager.setTypeFilter('^newtype$');

    const filtered = manager.filterIndicesAndTypes(allIndices);

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

    const filtered = manager.filterIndicesAndTypes(allIndices);

    expect(_.size(filtered)).to.eql(1);

    expect(_.size(filtered[0].types)).to.eql(1);
    expect(filtered[0].types).to.include('newtype_3');
  });

  it('reject filters that are not regex, function, or module with a function', ()=> {
    let throws = ()=> {
      manager.getFilterFunction({stuff: 'something'});
    };
    expect(throws).to.throw(/could not be interpreted\. Must be a path to a module, regex or function/);

    throws = ()=> {
      manager.getFilterFunction(()=> {});
    };
    expect(throws).to.throw(/filter function must take at least one argument/);

    throws = ()=> {
      manager.getFilterFunction(`${__dirname}/testMutators/dataMutator.js`);
    };
    expect(throws).to.throw(/was interpreted as a path and module does not return a function\. Must be a path to a module, regex or function/);

    throws = ()=> {
      manager.getFilterFunction(`${__dirname}/testMutators/notJs.txt`);
    };
    expect(throws).to.throw(/was interpreted as a path to a non-js file\. Must be a path to a module, regex or function/);

    throws = ()=> {
      manager.getFilterFunction(`${__dirname}/testMutators/nonexistent.js`);
    };
    expect(throws).to.throw(/was interpreted as a path and cannot be found\. Must be a path to a module, regex or function/);
  });

  it('should not accept index comparator that is not a function', ()=> {
    const throws = ()=> {
      manager.setIndexComparator({});
    };

    expect(throws).to.throw(/comparator must be a function that takes 2 arguments/);
  });

  it('should not accept index comparator does not take two arguments', ()=> {
    let throws = ()=> {
      manager.setIndexComparator(()=> {});
    };

    expect(throws).to.throw(/comparator must be a function that takes 2 arguments/);

    throws = ()=> {
      manager.setIndexComparator((arg)=> {});
    };

    expect(throws).to.throw(/comparator must be a function that takes 2 arguments/);

    throws = ()=> {
      manager.setIndexComparator((arg, arg2, arg3)=> {});
    };

    expect(throws).to.throw(/comparator must be a function that takes 2 arguments/);
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

  it('should prep job backlog considering completed jobs', (done)=> {
    manager.setIndexComparator((a, b)=> {
      return a.localeCompare(b);
    });

    const completedJob = {
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

  it('should prep job backlog and ignore completed', (done)=> {
    manager.setIndexComparator((a, b)=> {
      return a.localeCompare(b);
    });

    const completedJob = {
      index: 'myindex2',
      type:  'mytype1',
      count: 10
    };

    manager.completeJob(completedJob).then(()=> {
      return addData(source);
    }).then(()=> {
      return manager.initialize('*', true);
    }).then(()=> {
      return manager.fetchJob();
    }).then((job)=> {
      expect(job.index).to.eql('myindex1');
      return manager.fetchJob();
    }).then((job)=> {
      expect(job.index).to.eql('myindex1');
      return manager.fetchJob();
    }).then((job)=> {
      expect(job.index).to.eql('myindex2');
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

  it('should prep job backlog with no completed jobs', (done)=> {
    manager.setIndexComparator((a, b)=> {
      return a.localeCompare(b);
    });

    addData(source).then(()=> {
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
      expect(job.index).to.eql('myindex2');
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
    const jobs = [
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

  it('should not add the same job twice', (done)=> {
    const jobs = [
      {
        index: 'index1',
        type:  'type1',
        count: 22
      },
      {
        index: 'index1',
        type:  'type1',
        count: 22
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
    const jobs = [
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

  it('should get completed job count', (done)=> {
    const jobs = [
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
      return manager.getCompletedCount();
    }).then((completedCount)=> {
      expect(completedCount).to.eql(166);
      done();
    });
  });

  it('should clear completed jobs', (done)=> {
    const jobs = [
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
      return manager.clearCompletedJobs();
    }).then(()=> {
      return manager.getCompletedCount();
    }).then((completedCount)=> {
      expect(completedCount).to.eql(0);
      done();
    });
  });

  it('should return an empty array when there are no completed jobs', (done)=> {
    manager.getCompletedJobs().then((jobs)=> {
      expect(jobs).to.eql([]);
      done();
    });
  });

  it('should return an empty array when there are no backlog jobs', (done)=> {
    manager.getBacklogJobs().then((jobs)=> {
      expect(jobs).to.eql([]);
      done();
    });
  });

  it('should return all backlog jobs', (done)=> {
    const jobs = [
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

  it('should clear all backlog jobs', (done)=> {
    const jobs = [
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
      return manager.clearBacklogJobs();
    }).then(()=> {
      return manager.getBacklogJobs();
    }).then((backlogJobs)=> {
      expect(backlogJobs).to.eql([]);
      done();
    });
  });

  it('should return count of zero for empty completed', (done)=> {
    manager.getCompletedCount().then((count)=> {
      expect(count).to.eql(0);
      done();
    });
  });

  it('should return count of zero for empty backlog', (done)=> {
    manager.getBacklogCount().then((count)=> {
      expect(count).to.eql(0);
      done();
    });
  });

  it('should return total count of jobs in backlog', (done)=> {
    const jobs = [
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
      const jobTotal = _.reduce(jobs, (total, job)=> {
        total += job.count;
        return total;
      }, 0);

      expect(backlogTotal).to.eql(jobTotal);
      done();
    });
  });

  it('should get counts for provided jobs', (done)=> {
    const jobs = [
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
    }).finally(()=> {
      done();
    });
  });
});