import { expect } from 'chai';
import Transfer from '../lib/transfer';
import createEsClient from '../config/elasticsearch.js'
import config from '../config';
import _ from 'lodash';

var log = config.log;

import Promise from 'bluebird';
Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection(function (error) {
  log.error('Likely error: ', error.stack);
});

describe('transfer', function () {
  this.timeout(5000);

  var transfer = null;
  var source   = null;
  var dest     = null;

  before((done)=> {
    source = createEsClient('localhost:9200', '1.4');
    dest   = createEsClient('localhost:9201', '2.2');

    transfer = new Transfer(source, dest);

    transfer.source.indices.deleteTemplate({name: '*'}).finally(()=> {
      return transfer.dest.indices.deleteTemplate({name: '*'});
    }).finally(()=> {
      return transfer.source.indices.delete({index: '*'});
    }).finally(()=> {
      return transfer.dest.indices.delete({index: '*'});
    }).finally(()=> { done(); });
  });

  var addTemplate = ()=> {
    return transfer.source.indices.putTemplate({
      name: 'test_template',
      body: {
        template: "te*",
        refresh:  true,
        settings: {
          number_of_shards: 1
        },
        mappings: {
          type1: {
            _source:    {
              enabled: false
            },
            properties: {
              host_name:  {
                type: "keyword"
              },
              created_at: {
                type:   "date",
                format: "EEE MMM dd HH:mm:ss Z YYYY"
              }
            }
          }
        }
      }
    });
  };

  var addData = ()=> {
    return transfer.source.bulk({
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
        {someField2: 'somedata3'},
      ]
    });
  };

  var addLotsOfData = ()=> {
    return transfer.source.bulk({
      refresh: true,
      body:    require('./lotsOfData.json')
    });
  };

  it('should throw if getTemplates arg is not a non-zero string', () => {
    let throws = ()=>{
      transfer.getTemplates({});
    };
    expect(throws).to.throw(/targetTemplates must be string with length/);

    throws = ()=>{
      transfer.getTemplates(()=>{});
    };
    expect(throws).to.throw(/targetTemplates must be string with length/);

    throws = ()=>{
      transfer.getTemplates(1);
    };
    expect(throws).to.throw(/targetTemplates must be string with length/);

    throws = ()=>{
      transfer.getTemplates('');
    };
    expect(throws).to.throw(/targetTemplates must be string with length/);
  });

  it('should reject if there are no templates', (done) => {
    transfer.getTemplates('*').then(()=> {
      done('fail');
    }).catch((error)=> {
      expect(error).to.match(/Templates asked to be copied, but none found/);
      done();
    });
  });

  it('should get templates', (done) => {
    addTemplate().then(()=> {
        return transfer.getTemplates('*').then((templates)=> {
          expect(templates).to.be.instanceOf(Array);
          expect(_.size(templates)).to.eql(1);
          expect(templates[0].name).to.eql('test_template');
          expect(templates[0].template).to.eql('te*');
          done();
        });
      })
      .catch((error)=> { done('fail' + error); });
  });

  it('should put templates', (done)=> {
    var sourceTemplates = [
      {
        name:     'test_template',
        template: "te*",
        refresh:  true,
        settings: {
          number_of_shards: 1
        },
        mappings: {
          type1: {
            _source:    {
              enabled: false
            },
            properties: {
              host_name:  {
                type: "keyword"
              },
              created_at: {
                type:   "date",
                format: "EEE MMM dd HH:mm:ss Z YYYY"
              }
            }
          }
        }
      },
      {
        name:     'test_template_2',
        template: "te2*",
        refresh:  true,
        settings: {
          number_of_shards: 1
        },
        mappings: {
          type1: {
            _source:    {
              enabled: false
            },
            properties: {
              host_name:  {
                type: "keyword"
              },
              created_at: {
                type:   "date",
                format: "EEE MMM dd HH:mm:ss Z YYYY"
              }
            }
          }
        }
      }
    ];

    transfer.putTemplates(sourceTemplates).then(()=> {
      return transfer.dest.indices.getTemplate({name: '*'});
    }).then((destTemplates)=> {
      expect(_.size(destTemplates)).to.eql(2);
      expect(destTemplates).to.have.property('test_template');
      expect(destTemplates.test_template.template).to.eql('te*');
      expect(destTemplates).to.have.property('test_template_2');
      expect(destTemplates.test_template_2.template).to.eql('te2*');
      done();
    });
  });

  it('should get indices', (done)=> {
    var index = {
      settings: {
        number_of_shards:   1,
        number_of_replicas: 2
      },
      mappings: {
        type1: {
          properties: {
            field1: {type: "string"}
          }
        }
      },
      aliases:  {
        alias_1: {}
      }
    };

    transfer.source.indices.create({
      index: 'twitter1',
      body:  index
    }).then(()=> {
      return transfer.source.indices.create({
        index: 'twitter2',
        body:  index
      });
    }).then(()=> {
      return transfer.getIndices('*');
    }).then((indices)=> {
      expect(indices).to.have.length(2);
      expect(Object.keys(indices[0])).to.include('name', 'settings', 'mappings', 'alias', 'warmers');
      expect(indices[0].name).to.be.oneOf([
        'twitter1',
        'twitter2'
      ]);
      expect(Object.keys(indices[1])).to.include('name', 'settings', 'mappings', 'alias', 'warmers');
      expect(indices[1].name).to.be.oneOf([
        'twitter1',
        'twitter2'
      ]);
      expect(indices[0].name).to.not.eql(indices[1].name);
      done();
    });
  });

  it('should reject if there is an error during get indices', (done)=> {
    transfer.getIndices('missingIndexName').then(()=>{
      done('fail');
    }).catch(()=>{
      done();
    });
  });

  it('should put indices', (done)=> {
    var index = {
      settings: {
        number_of_shards:   1,
        number_of_replicas: 2
      },
      mappings: {
        type1: {
          properties: {
            field1: {type: "string"}
          }
        }
      },
      aliases:  {
        alias_1: {}
      }
    };

    transfer.dest.indices.delete({index: '*'}).then(()=> {
      return transfer.source.indices.create({
        index: 'twitter1',
        body:  index
      });
    }).then(()=> {
      return transfer.source.indices.create({
        index: 'twitter2',
        body:  index
      });
    }).then(()=> {
      return transfer.getIndices('*');
    }).then((indices)=> {
      return transfer.putIndices(indices);
    }).then(()=> {
      return transfer.dest.indices.get({index: '*'});
    }).then((response)=> {
      expect(_.size(response)).to.eql(2);
      expect(response.twitter1).to.be.defined;
      expect(response.twitter1.settings.index.number_of_shards).to.eql('1');
      expect(response.twitter1.settings.index.number_of_replicas).to.eql('2');
      expect(response.twitter1.mappings.type1.properties.field1.type).to.eql('string');
      expect(response.twitter2).to.be.defined;
      expect(response.twitter2.settings.index.number_of_shards).to.eql('1');
      expect(response.twitter2.settings.index.number_of_replicas).to.eql('2');
      expect(response.twitter2.mappings.type1.properties.field1.type).to.eql('string');
      done();
    }).catch((error)=> {done(error);});
  });

  it('should reject if there is an error during put indices', (done)=> {
    var indices = [{
      name: 'something',
      settings: {
        number_of_shards:   -100
      },
      mappings: {
        type1: {
          properties: {
            field1: {type: "string"}
          }
        }
      },
      aliases:  {
        alias_1: {}
      }
    }];

    transfer.putIndices(indices).then(()=> {
      done('fail');
    }).catch(()=> {
      done();
    });
  });

  it('should get all data in given index and type', (done)=> {
    addLotsOfData().then(()=> {
      return transfer.transferData('myindex1', 'mytype1', {});
    }).then(()=> {
      return transfer.dest.indices.refresh({index: ''})
    }).then(()=> {
      return transfer.dest.search({size: 100});
    }).then((response)=> {
      expect(response.hits.hits).to.be.length(20);
      done();
    }).catch((error)=> {
      done(error);
    });
  });

  it('should throw if no indexName is provided', (done)=> {
    let throws = ()=> {
      transfer.transferData(null, 'mytype1');
    };

    expect(throws).to.throw(/targetIndex must be string with length/);
    done();
  });

  it('should throw if no typeName is provided', (done)=> {
    let throws = ()=> {
      transfer.transferData('myindex', null);
    };

    expect(throws).to.throw(/targetType must be string with length/);
    done();
  });

  it('should throw if body is not an object', (done)=> {
    let throws = ()=> {
      transfer.transferData('myindex', 'mytype', 'body');
    };

    expect(throws).to.throw(/if provided, body must be an object/);
    done();
  });

  it('should reject if index does not exist', (done)=> {
    transfer.transferData('notthere', 'mytype1').then(()=>{
      done('fail');
    }).catch(()=>{
      done();
    });
  });

  it('should call callback with status updates', (done)=> {
    var data = [];

    _.times(100, (n)=> {
      data.push({
        index: {
          _index: 'myindex1',
          _type:  'myindex1'
        }
      });
      data.push({something: 'data' + n});
    });

    transfer.setUpdateCallback((status)=> {
      log.info('status', status);
      if (status.transferred === 100) {
        done();
      }
    });

    source.bulk({body: data}).then((results)=> {
      if (results.errors) {
        log.error('errors', results);
        return Promise.reject('errors: ' + results.errors);
      }

      return source.indices.refresh();
    }).then(()=> {
      return transfer.transferData('myindex1', 'myindex1');
    });
  });

  it('load all mutators from directory', ()=> {
    expect(_.size(transfer.getMutators())).to.eql(0);

    transfer.loadMutators(__dirname + '/testMutators');

    let mutators = transfer.getMutators();
    expect(_.size(mutators)).to.eql(3);
    expect(mutators['data']).to.have.length(1);
    expect(mutators['data'][0].type).to.eql('data');
    expect(_.isFunction(mutators['data'][0].predicate)).to.be.true;
    expect(_.isFunction(mutators['data'][0].mutate)).to.be.true;
    expect(mutators['template']).to.have.length(1);
    expect(mutators['template'][0].type).to.eql('template');
    expect(_.isFunction(mutators['template'][0].predicate)).to.be.true;
    expect(_.isFunction(mutators['template'][0].mutate)).to.be.true;
    expect(mutators['index']).to.have.length(1);
    expect(mutators['index'][0].type).to.eql('index');
    expect(_.isFunction(mutators['index'][0].predicate)).to.be.true;
    expect(_.isFunction(mutators['index'][0].mutate)).to.be.true;
  });

  it('should load a specific mutator', ()=> {
    expect(_.size(transfer.getMutators())).to.eql(0);

    transfer.loadMutators(__dirname + '/testMutators/dataMutator.js');

    let mutators = transfer.getMutators();
    expect(_.size(mutators)).to.eql(1);
    expect(mutators['data']).to.have.length(1);
    expect(mutators['data'][0].type).to.eql('data');
    expect(_.isFunction(mutators['data'][0].predicate)).to.be.true;
    expect(_.isFunction(mutators['data'][0].mutate)).to.be.true;
  });

  it('should not load a directory with malformed mutators', ()=> {
    expect(_.size(transfer.getMutators())).to.eql(0);

    let throws = ()=> {
      transfer.loadMutators(__dirname + '/invalidMutators');
    };

    expect(throws).to.throw();
  });

  it('should not load a mutator with no mutate function', ()=> {
    expect(_.size(transfer.getMutators())).to.eql(0);

    let throws = ()=> {
      transfer.loadMutators(__dirname + '/invalidMutators/noMutateMutator.js');
    };

    expect(throws).to.throw(/Mutator mutate\(\) not provided/);
  });

  it('should not load a mutator with no predicate function', ()=> {
    expect(_.size(transfer.getMutators())).to.eql(0);

    let throws = ()=> {
      transfer.loadMutators(__dirname + '/invalidMutators/noPredicateMutator.js');
    };

    expect(throws).to.throw(/Mutator predicate\(\) not provided/);
  });

  it('should not load a mutator with no type', ()=> {
    expect(_.size(transfer.getMutators())).to.eql(0);

    let throws = ()=> {
      transfer.loadMutators(__dirname + '/invalidMutators/noTypeMutator.js');
    };

    expect(throws).to.throw(/Mutator type string not provided/);
  });

  it('should not load a mutator with invalid type', ()=> {
    expect(_.size(transfer.getMutators())).to.eql(0);

    let throws = ()=> {
      transfer.loadMutators(__dirname + '/invalidMutators/invalidType.js');
    };

    expect(throws).to.throw(/Mutator type 'wrong' not one of/);
  });

  it('should not load a mutator that isn\'t js', ()=> {
    expect(_.size(transfer.getMutators())).to.eql(0);

    let throws = ()=> {
      transfer.loadMutators(__dirname + '/invalidMutators/notAJsFile');
    };

    expect(throws).to.throw(/No \.js file\(s\) at/);
  });

  it('should return the original when no mutator is present', (done)=>{
    source.indices.create({
      index: 'index_to_mutate',
      body:  {settings: {number_of_shards: 4}}
    }).then(()=> {
      return transfer.transferIndices('index_to_mutate');
    }).then(()=> {
      return dest.indices.refresh();
    }).then(()=> {
      return dest.indices.get({index: 'index_to_mutate'});
    }).then((index)=> {
      expect(index.index_to_mutate.settings.index.number_of_shards).to.eql('4');
      done();
    });
  });

  it('should return the original when the mutator does not apply', (done)=> {
    transfer.loadMutators(__dirname + '/testMutators/indexMutator.js');

    source.indices.create({
      index: 'index_not_to_mutate',
      body:  {settings: {number_of_shards: 4}}
    }).then(()=> {
      return transfer.transferIndices('index_not_to_mutate');
    }).then(()=> {
      return dest.indices.refresh();
    }).then(()=> {
      return dest.indices.get({index: 'index_not_to_mutate'});
    }).then((index)=> {
      expect(index.index_not_to_mutate.settings.index.number_of_shards).to.eql('4');
      done();
    });
  });

  it('should use index mutator to change index during transfer', (done)=> {
    transfer.loadMutators(__dirname + '/testMutators/indexMutator.js');

    source.indices.create({
      index: 'index_to_mutate',
      body:  {settings: {number_of_shards: 4}}
    }).then(()=> {
      return transfer.transferIndices('index_to_mutate');
    }).then(()=> {
      return dest.indices.refresh();
    }).then(()=> {
      return dest.indices.get({index: 'new_index_name'});
    }).then((index)=> {
      expect(index.new_index_name.settings.index.number_of_shards).to.eql('4');
      return dest.indices.get({index: 'index_to_mutate'}).catch((error)=> {
        expect(error.status).to.eql(404);
        return 'not found';
      });
    }).then((result)=> {
      expect(result).to.eql('not found');
      done();
    });
  });

  it('should use a template mutator to change template during transfer', (done)=> {
    transfer.loadMutators(__dirname + '/testMutators/templateMutator.js');

    source.indices.putTemplate({
      name: 'test_template',
      body: {template: 'template_this*'}
    }).then(()=> {
      return transfer.transferTemplates('test_template');
    }).then(()=> {
      return dest.indices.getTemplate({name: 'test_template'});
    }).then((template)=> {
      expect(template.test_template.template).to.eql('template_that*');
      done();
    });
  });

  it('should use a data mutator to change documents during transfer', (done)=> {
    transfer.loadMutators(__dirname + '/testMutators/dataMutator.js');

    source.create({
      index: 'something_1990-05-21',
      type:  'sometype',
      body:  {field: 'daata'}
    }).then(()=> {
      return source.indices.refresh();
    }).then(()=> {
      return transfer.transferData('something_1990-05-21', 'sometype');
    }).then(()=> {
      return dest.indices.refresh();
    }).then(()=> {
      return dest.search({index: 'something_1990*'});
    }).then((document)=> {
      expect(document.hits.hits.length).to.eql(1);
      expect(document.hits.hits[0]._index).to.eql('something_1990-05');
      expect(document.hits.hits[0]._source.field).to.eql('daata');
      done();
    });
  });

  // it('should recover from some errors', (done)=>{
  //   let results = {
  //     errors: 5,
  //     items: [
  //       {
  //         update: {
  //           status: 'success'
  //         }
  //       },
  //       {
  //         update: {
  //           error: {
  //             type: 'es_rejected_execution_exception'
  //           }
  //         }
  //       }
  //     ]
  //   };
  //
  //   let bulkBody = [
  //     { update: { _index: 'something', _type: 'type'}},
  //     { field: 'data1'},
  //     { update: { _index: 'something2', _type: 'type2'}},
  //     { field: 'data2'}
  //   ];
  //
  //   transfer.handleBulkErrors(results, bulkBody).then();
  // });

  afterEach((done)=> {
    transfer.clearMutators();
    transfer.setUpdateCallback(null);

    transfer.source.indices.deleteTemplate({name: '*'}).finally(()=> {
      return transfer.dest.indices.deleteTemplate({name: '*'});
    }).finally(()=> {
      return transfer.source.indices.delete({index: '*'});
    }).finally(()=> {
      return transfer.dest.indices.delete({index: '*'});
    }).finally(()=> { done(); });
  });
});