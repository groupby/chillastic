/*eslint no-magic-numbers: "off" */
const services = require('../services');
const utils    = require('../../config/utils');

const getHealth = (req, res) => {
  const status = [
    services.manager.isRunning(),
    services.manager.getWorkersStatus()
  ];

  Promise.all(status).then(statuses => {
    const response = {
      manager: statuses[0] ? 'running' : 'stopped',
      workers: statuses[1]
    };

    res.status(200).json(response);
  }).catch(error => utils.processError(error, res));
};

module.exports = {
  getHealth: getHealth
};