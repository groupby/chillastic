/*eslint no-magic-numbers: "off"*/
/*eslint no-invalid-this: "off"*/
const _              = require('lodash');
const expect         = require('chai').expect;
const Promise        = require('bluebird');
const TestConfig     = require('../config');
const Utils          = require('../utils');
const Compiler       = require('../../app/services/compiler');
const Transfer       = require('../../app/services/transfer');
const createEsClient = require('../../config/elasticsearch.js');
const config         = require('../../config/index');

const log      = config.log;
const compiler = new Compiler();
const utils    = new Utils();

Promise.longStackTraces();
Promise.onPossiblyUnhandledRejection((error) => log.error('Likely error: ', error.stack));

const loadMutator = (path) => compiler.compile(utils.loadFile(path));

describe('transfer', function () {
  this.timeout(5000);

  let transfer = null;
  let source   = null;
  let dest     = null;

  before((done) => {
    source = createEsClient(TestConfig.elasticsearch.source);
    dest = createEsClient(TestConfig.elasticsearch.destination);

    transfer = new Transfer(source, dest);

    utils.deleteAllTemplates(source)
    .finally(() => utils.deleteAllTemplates(dest))
    .finally(() => utils.deleteAllIndices(source))
    .finally(() => utils.deleteAllIndices(dest))
    .finally(() => done());
  });

  afterEach((done) => {
    transfer.clearMutators();
    transfer.setUpdateCallback(null);

    utils.deleteAllTemplates(source)
    .finally(() => utils.deleteAllTemplates(dest))
    .finally(() => utils.deleteAllIndices(source))
    .finally(() => utils.deleteAllIndices(dest))
    .finally(() => done());
  });

  const addTemplate = () => transfer.source.indices.putTemplate({
    name: 'test_template',
    body: {
      template: 'te*',
      refresh:  true,
      settings: {
        number_of_shards: 1
      },
      mappings: {
        type1: {
          _source: {
            enabled: false
          },
          properties: {
            host_name: {
              type: 'keyword'
            },
            created_at: {
              type:   'date',
              format: 'EEE MMM dd HH:mm:ss Z YYYY'
            }
          }
        }
      }
    }
  });

  it('should get all indices and types', (done) => {
    utils.addData(source)
    .then(() => source.indices.refresh())
    .then(() => Transfer.getIndices(source, '*'))
    .then((indices) => {
      expect(_.size(indices)).to.be.equals(3);

      const myindex1 = _.find(indices, {name: 'myindex1'});
      expect(myindex1).to.not.be.undefined;
      expect(_.size(myindex1.mappings)).to.be.equals(1);
      expect(myindex1.mappings.mytype1).to.not.be.undefined;
      expect(myindex1.mappings.mytype2).to.be.undefined;

      const myindex2 = _.find(indices, {name: 'myindex2'});
      expect(myindex2).to.not.be.undefined;
      expect(_.size(myindex2.mappings)).to.be.equals(1);
      expect(myindex2.mappings.mytype1).to.not.be.undefined;

      const myindex3 = _.find(indices, {name: 'myindex3'});
      expect(myindex3).to.not.be.undefined;
      expect(_.size(myindex1.mappings)).to.be.equals(1);
      expect(myindex3.mappings.mytype2).to.be.undefined;
      expect(myindex3.mappings.mytype3).to.not.be.undefined;
    })
    .then(() => done())
    .catch(done);
  });

  it('should throw if getTemplates arg is not a non-zero string', (done) => {
    Transfer.getTemplates(source, {})
    .then(() => done('fail'))
    .catch((e) => expect(e.message).to.be.equals('targetTemplates must be string with length'))
    .then(() => Transfer.getTemplates(source, () => {
    }))
    .then(() => done('fail'))
    .catch((e) => expect(e.message).to.be.equals('targetTemplates must be string with length'))
    .then(() => Transfer.getTemplates(source, 1))
    .then(() => done('fail'))
    .catch((e) => expect(e.message).to.be.equals('targetTemplates must be string with length'))
    .then(() => Transfer.getTemplates(source, ''))
    .then(() => done('fail'))
    .catch((e) => expect(e.message).to.be.equals('targetTemplates must be string with length'))
    .then(() => done());
  });

  it('should reject if there are no templates', (done) => {
    Transfer.getTemplates(source, '*')
    .then(() => done('fail'))
    .catch((error) => {
      expect(error).to.match(/Templates asked to be copied, but none found/);
      done();
    });
  });

  it('should get templates', (done) => {
    addTemplate()
    .then(() => Transfer.getTemplates(source, '*'))
    .then((templates) => {
      expect(templates).to.be.an.instanceof(Array);
      expect(_.size(templates)).to.be.equals(1);
      expect(templates[0].name).to.be.equals('test_template');
      expect(templates[0].index_patterns[0]).to.be.equals('te*');
      done();
    })
    .catch(done);
  });

  it('should put templates', (done) => {
    const sourceTemplates = [
      {
        name:     'test_template',
        template: 'te*',
        refresh:  true,
        settings: {
          number_of_shards: 1
        },
        mappings: {
          type1: {
            _source: {
              enabled: false
            },
            properties: {
              host_name: {
                type: 'text'
              },
              created_at: {
                type:   'date',
                format: 'EEE MMM dd HH:mm:ss Z YYYY'
              }
            }
          }
        }
      },
      {
        name:     'test_template_2',
        template: 'te2*',
        refresh:  true,
        settings: {
          number_of_shards: 1
        },
        mappings: {
          type1: {
            _source: {
              enabled: false
            },
            properties: {
              host_name: {
                type: 'text'
              },
              created_at: {
                type:   'date',
                format: 'EEE MMM dd HH:mm:ss Z YYYY'
              }
            }
          }
        }
      }
    ];

    transfer.putTemplates(sourceTemplates)
    .then(() => transfer.dest.indices.getTemplate())
    .then((destTemplates) => {
      expect(destTemplates).to.have.property('test_template');
      expect(destTemplates.test_template.index_patterns[0]).to.be.equals('te*');
      expect(destTemplates).to.have.property('test_template_2');
      expect(destTemplates.test_template_2.index_patterns[0]).to.be.equals('te2*');
      done();
    })
    .catch(done);
  });

  it('should get indices', (done) => {
    const index = {
      settings: {
        number_of_shards:   1,
        number_of_replicas: 2
      },
      mappings: {
        type1: {
          properties: {
            field1: {type: 'text'}
          }
        }
      },
      aliases: {
        alias_1: {}
      }
    };

    transfer.source.indices.create({
      index: 'twitter1',
      body:  index
    })
    .then(() => transfer.source.indices.create({
      index: 'twitter2',
      body:  index
    }))
    .then(() => Transfer.getIndices(transfer.source, '*'))
    .then((indices) => {
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
    })
    .then(() => done());
  });

  it('should reject if there is an error during get indices', (done) => {
    Transfer.getIndices(transfer.source, 'missingIndexName')
    .then(() => done('fail'))
    .catch(() => done());
  });

  it('should put indices', (done) => {
    const index = {
      settings: {
        number_of_shards:   1,
        number_of_replicas: 2
      },
      mappings: {
        type1: {
          properties: {
            field1: {type: 'long'}
          }
        }
      },
      aliases: {
        alias_1: {}
      }
    };

    utils.deleteAllIndices(transfer.dest)
    .then(() => transfer.source.indices.create({
      index: 'twitter1',
      body:  index
    }))
    .then(() => transfer.source.indices.create({
      index: 'twitter2',
      body:  index
    }))
    .then(() => Transfer.getIndices(transfer.source, '*'))
    .then((indices) => transfer.putIndices(indices))
    .then(() => transfer.dest.indices.get({index: '*'}))
    .then((response) => {
      expect(_.size(response)).to.be.equals(2);

      expect(response.twitter1).to.not.be.undefined;
      expect(response.twitter1.settings.index.number_of_shards).to.be.equals('1');
      expect(response.twitter1.settings.index.number_of_replicas).to.be.equals('2');
      expect(response.twitter1.mappings.type1.properties.field1.type).to.be.equals('long');

      expect(response.twitter2).to.not.be.undefined;
      expect(response.twitter2.settings.index.number_of_shards).to.be.equals('1');
      expect(response.twitter2.settings.index.number_of_replicas).to.be.equals('2');
      expect(response.twitter2.mappings.type1.properties.field1.type).to.be.equals('long');
    })
    .then(() => done())
    .catch(done);
  });

  it('should reject if there is an error during put indices', (done) => {
    const indices = [
      {
        name:     'something',
        settings: {
          number_of_shards: -100
        },
        mappings: {
          type1: {
            properties: {
              field1: {type: 'text'}
            }
          }
        },
        aliases: {
          alias_1: {}
        }
      }
    ];

    transfer.putIndices(indices)
    .then(() => done('fail'))
    .catch(() => done());
  });

  it('should get all data in given index and type', (done) => {
    const index = 'myindex1';
    const type  = 'mytype1';

    utils.createIndices(transfer, index, index)
    .then(() => source.bulk({
      refresh: true,
      body:    require('./lotsOfData.json')
    }))
    .then(() => transfer.transferData(index, type, 10))
    .then(() => transfer.dest.indices.refresh({index: '*'}))
    .then(() => transfer.dest.search({size: 100}))
    .then((response) => expect(response.hits.hits).to.be.length(20))
    .then(() => done())
    .catch(done);
  });

  it('should throw if no indexName is provided', (done) => {
    const throws = () => transfer.transferData(null, 'mytype1');
    expect(throws).to.throw('targetIndex must be string with length');
    done();
  });

  it('should throw if no typeName is provided', (done) => {
    const throws = () => transfer.transferData('myindex', null);
    expect(throws).to.throw('targetType must be string with length');
    done();
  });

  it('should reject if index does not exist', (done) => {
    transfer.transferData('notthere', 'mytype1')
    .then(() => done('fail'))
    .catch(() => done());
  });

  it('should call callback with status updates', (done) => {
    const index = 'myindex1';
    const data  = [];

    _.times(100, (n) => {
      data.push({
        index: {
          _index: index,
          _type:  index
        }
      });
      data.push({something: `data${n}`});
    });

    transfer.setUpdateCallback((status) => {
      log.info('status', status);
      if (status.transferred === 100) {
        done();
      }
    });

    utils.createIndices(transfer, index, index)
    .then(() => source.bulk({body: data}))
    .then((results) => results.errors ? Promise.reject(`errors: ${results.errors}`) : source.indices.refresh())
    .then(() => transfer.transferData(index, index))
    .catch(done);
  });

  it('should return the original when no mutator is present', (done) => {
    source.indices.create({
      index: 'index_to_mutate',
      body:  {settings: {number_of_shards: 4}}
    })
    .then(() => transfer.transferIndices('index_to_mutate'))
    .then(() => dest.indices.refresh())
    .then(() => dest.indices.get({index: 'index_to_mutate'}))
    .then((index) => expect(index.index_to_mutate.settings.index.number_of_shards).to.be.equals('4'))
    .then(() => done());
  });

  it('should return the original when the mutator does not apply', (done) => {
    transfer.setMutators({index: [loadMutator(`${__dirname}/validMutators/index.js`)]});

    source.indices.create({
      index: 'index_not_to_mutate',
      body:  {settings: {number_of_shards: 4}}
    })
    .then(() => transfer.transferIndices('index_not_to_mutate'))
    .then(() => dest.indices.refresh())
    .then(() => dest.indices.get({index: 'index_not_to_mutate'}))
    .then((index) => expect(index.index_not_to_mutate.settings.index.number_of_shards).to.be.equals('4'))
    .then(() => done())
    .catch(done);
  });

  it('should use index mutator to change index during transfer', (done) => {
    transfer.setMutators({index: [loadMutator(`${__dirname}/validMutators/index.js`)]});

    source.indices.create({
      index: 'index_to_mutate',
      body:  {settings: {number_of_shards: 4}}
    })
    .then(() => transfer.transferIndices('index_to_mutate'))
    .then(() => dest.indices.refresh())
    .then(() => dest.indices.get({index: 'new_index_name'}))
    .then((index) => {
      expect(index.new_index_name.settings.index.number_of_shards).to.be.equals('4');
      return dest.indices.get({index: 'index_to_mutate'}).catch((error) => {
        expect(error.status).to.be.equals(404);
        return 'not found';
      });
    })
    .then((result) => expect(result).to.be.equals('not found'))
    .then(() => done())
    .catch(done);
  });

  it('should call mutator with arguments', (done) => {
    const mutator     = loadMutator(`${__dirname}/validMutators/indexWithArgs.js`);
    mutator.arguments = {
      name:   'creative',
      target: 'index_to_mutate'
    };

    transfer.setMutators({index: [mutator]});

    source.indices.create({
      index: 'index_to_mutate',
      body:  {settings: {number_of_shards: 4}}
    })
    .then(() => transfer.transferIndices('index_to_mutate'))
    .then(() => dest.indices.refresh())
    .then(() => dest.indices.get({index: 'creative'}))
    .then((index) => {
      expect(index.creative.settings.index.number_of_shards).to.be.equals('4');
      return dest.indices.get({index: 'index_to_mutate'}).catch((error) => {
        expect(error.status).to.be.equals(404);
        return 'not found';
      });
    })
    .then((result) => expect(result).to.be.equals('not found'))
    .then(() => done())
    .catch(done);
  });

  it('should use a template mutator to change template during transfer', (done) => {
    transfer.setMutators({template: [loadMutator(`${__dirname}/validMutators/template.js`)]});

    source.indices.putTemplate({
      name: 'test_template',
      body: {template: 'template_this*'}
    })
    .then(() => transfer.transferTemplates('test_template'))
    .then(() => dest.indices.getTemplate({name: 'test_template'}))
    .then((template) => expect(template.test_template.index_patterns[0]).to.be.equals('template_that*'))
    .then(() => done())
    .catch(done);
  });

  it('should use a data mutator to change documents during transfer', (done) => {
    transfer.setMutators({data: [loadMutator(`${__dirname}/validMutators/data.js`)]});

    const srcIndex = 'something_1990-05-21';
    const dstIndex = 'something_1990-05';
    const type     = 'sometype';

    utils.createIndices(transfer, srcIndex, dstIndex)
    .then(() => source.index({
      index: srcIndex,
      type:  type,
      body:  {field: 'daata'}
    }))
    .then(() => source.indices.refresh())
    .then(() => transfer.transferData(srcIndex, type))
    .then(() => dest.indices.refresh())
    .then(() => dest.search({index: 'something_1990*'}))
    .then((document) => {
      expect(document.hits.hits.length).to.be.equals(1);
      expect(document.hits.hits[0]._index).to.be.equals('something_1990-05');
      expect(document.hits.hits[0]._source.field).to.be.equals('daata');
    })
    .then(() => done())
    .catch(done);
  });

  it('should use a data mutator to drop some documents during transfer', (done) => {
    const mutator     = loadMutator(`${__dirname}/validMutators/dropWithArgs.js`);
    mutator.arguments = {
      match: 'daata2'
    };
    transfer.setMutators({data: [mutator]});

    const index = 'something_1990-05-21';
    const type  = 'sometype';

    utils.createIndices(transfer, index, index)
    .then(() => source.index({
      index: index,
      type:  type,
      body:  {field: 'daata'}
    }))
    .then(() => source.index({
      index: index,
      type:  type,
      body:  {field: 'daata2'}
    }))
    .then(() => source.indices.refresh())
    .then(() => transfer.transferData(index, type))
    .then(() => dest.indices.refresh())
    .then(() => dest.search({index}))
    .then((document) => {
      expect(document.hits.hits.length).to.be.equals(1);
      expect(document.hits.hits[0]._index).to.be.equals(index);
      expect(document.hits.hits[0]._source.field).to.be.equals('daata');
    })
    .then(() => done())
    .catch(done);
  });
});