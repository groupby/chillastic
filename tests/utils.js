const _       = require('lodash');
const Promise = require('bluebird');
const fs      = require('fs');

const Utils = function () {
  const self = this;

  self.loadFile = (path) => fs.readFileSync(path, 'utf8');

  self.createIndices = (clients, src, dst) => {
    return clients.source.indices.create({index: src})
    .then(() => clients.dest.indices.create({index: dst}));
  };

  self.addData = (client) =>
      client.indices.create({index: 'myindex1'})
      .then(() => client.indices.create({index: 'myindex2'}))
      .then(() => client.indices.create({index: 'myindex3'}))
      .then(() => client.bulk({
        refresh: true,
        body:    [
          {index: {_index: 'myindex1', _type: 'mytype1'}},
          {someField1: 'somedata1'},
          {index: {_index: 'myindex1', _type: 'mytype1'}},
          {someField2: 'somedata2'},
          {index: {_index: 'myindex2', _type: 'mytype1'}},
          {someField1: 'somedata1'},
          {index: {_index: 'myindex3', _type: 'mytype3'}},
          {someField2: 'somedata2'},
          {index: {_index: 'myindex3', _type: 'mytype3'}},
          {someField2: 'somedata3'},
          {index: {_index: 'myindex3', _type: 'mytype3'}},
          {someField2: 'somedata3'}
        ]
      }));

  self.deleteAllTemplates = (client) =>
      client.indices.getTemplate()
      .then((templateNames) => Promise.all(_.keys(templateNames).map((t) => client.indices.deleteTemplate({name: t}))));

  self.deleteAllIndices = (client) =>
      client.indices.get({index: '_all'})
      .then((indexNames) => Promise.all(_.keys(indexNames).map((i) => client.indices.delete({index: i}))));
};

module.exports = Utils;
