const request    = require('supertest-as-promised');
const chai       = require('chai');
const expect     = chai.expect;
const asPromised = require('chai-as-promised');
chai.use(asPromised);
const HTTPStatus = require('http-status');
const TestConfig = require('./config');
const log        = require('../config').log;

describe('chillastic full routes', () => {
  log.level('debug');

  it('returns 400 response code when mutator src not found', (done) => {
    const task = {
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
    };

    const app   = require('../index')(TestConfig.redis.host, TestConfig.redis.port, 7001);
    const agent = request(app);
    agent.post('/tasks/doesNotExist')
      .send(task)
      .expect(HTTPStatus.BAD_REQUEST)
      .then((res) => expect(res.body.error).to.equal('Src for mutator id doesNotExist not found'))
      .then(() => done())
      .catch((err) => done(err))
      .finally(() => {
        app.services.manager.setRunning(false);
        app.services.worker.killStopped();
      });
  });

  it('returns 400 response code when filter src not found', (done) => {
    const task = {
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
    };

    const app   = require('../index')(TestConfig.redis.host, TestConfig.redis.port, 7001);
    const agent = request(app);
    agent.post('/tasks/doesNotExist')
      .send(task)
      .expect(HTTPStatus.BAD_REQUEST)
      .then((res) => expect(res.body.error).to.equal('Src for filter id doesNotExist not found'))
      .then(() => done())
      .catch((err) => done(err))
      .finally(() => {
        app.services.manager.setRunning(false);
        app.services.worker.killStopped();
        app.services.redis.quit();
      });
  });

});