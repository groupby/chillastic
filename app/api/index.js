const express    = require('express');
const router     = express.Router();
const controller = require('./root.controller');
const tasks = require('./tasks');

module.exports = () => {
  router.get('/status', controller.getStatus);

  router.use('/tasks', tasks());
  return router;
};