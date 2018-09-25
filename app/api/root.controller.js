const HttpStatus = require('http-status');
const services   = require('../services');
const utils      = require('../../config/utils');

module.exports = {
  /**
   * Get status of system
   */
  getStatus: (req, res) =>
    Promise.all([
      services.manager.isRunning(),
      services.manager.getWorkersStatus()
    ])
      .then((statuses) =>
        res.status(HttpStatus.OK).json({
          manager: statuses[0] ? 'running' : 'stopped',
          workers: statuses[1]
        })
      )
      .catch((e) => utils.processError(e, res)),

  /**
   * Start all workers
   */
  start: (req, res) =>
    services.manager.setRunning(true)
      .then(() => res.status(HttpStatus.OK).json())
      .catch((e) => utils.processError(e, res)),

  /**
   * Stop all workers
   */
  stop: (req, res) =>
    services.manager.setRunning(false)
      .then(() => res.status(HttpStatus.OK).json())
      .catch((e) => utils.processError(e, res))
};