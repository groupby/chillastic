const HttpStatus = require('http-status');
const ObjectId   = require('../../models/objectId');
const services   = require('../../services');
const utils      = require('../../../config/utils');

module.exports = {
  /**
   * Add a new filter by id
   * @param req
   * @param res
   */
  add: (req, res) =>
    new ObjectId({namespace: req.params.namespace, id: req.params.id}).validate()
      .then((objectId) => services.filters.add(objectId, req.body))
      .then(() => res.status(HttpStatus.OK).json())
      .catch((e) => utils.processError(e, res)),

  /**
   * Delete a filter by id
   * @param req
   * @param res
   */
  delete: (req, res) =>
    new ObjectId({namespace: req.params.namespace, id: req.params.id}).validate()
      .then((objectId) => services.filters.remove(objectId))
      .then(() => res.status(HttpStatus.NO_CONTENT).json())
      .catch((e) => utils.processError(e, res)),

  /**
   * Returns list of all filters in a namespace by id
   */
  getAllIdsByNamespace: (req, res) =>
    new ObjectId({namespace: req.params.namespace, id: 'dummy'}).validate()
      .then((objectId) => services.filters.getIds(objectId.namespace))
      .then((ids) => res.status(HttpStatus.OK).json({ids}))
      .catch((e) => utils.processError(e, res))
};