const HttpStatus = require('http-status');
const Promise    = require('bluebird');
const services   = require('../../services');
const config     = require('../../../config');
const utils      = require('../../../config/utils');

const PERCENTAGE = 100;

/**
 * Generate the status for a task given it's name
 * @param taskId
 * @returns {Promise.<TResult>}
 */
const getTaskStatus = (taskId) => {
  return Promise.all([services.subtasks.getTotal(taskId), services.subtasks.countBacklog(taskId), services.subtasks.countCompleted(taskId), services.tasks.getProgress(taskId)])
    .then((results) => {
      const total     = results[0];
      const completed = results[2];
      return {
        percentComplete: ((completed / total) * PERCENTAGE).toFixed(config.numDigits),
        total:           total,
        completed:       completed,
        backlog:         results[1],
        inProgress:      results[3],
      };
    });
};

module.exports = {
  /**
   * Returns list of all tasks, and their status/progress
   */
  getAll: (req, res) =>
    services.tasks.getAll()
      .then((taskIds) =>
        Promise.reduce(taskIds, (result, taskId) =>
          getTaskStatus(taskId)
            .then((status) => result[taskId] = status)
            .then(() => result), {}))
      .then((response) => res.status(HttpStatus.OK).json(response))
      .catch((error) => utils.processError(error, res)),

  /**
   * Get a specific task by name
   */
  get: (req, res) =>
    services.tasks.exists(req.params.id)
      .then((exists) =>
        exists
          ? getTaskStatus(req.params.id).then((status) => res.status(HttpStatus.OK).json(status))
          : res.status(HttpStatus.NOT_FOUND).json({error: `task '${req.params.id}' not found`})
      )
      .catch((e) => utils.processError(e, res)),

  /**
   * Add a new task by name
   */
  add: (req, res) =>
    services.tasks.add(req.params.id, req.body)
      .then(() => res.status(HttpStatus.OK).json())
      .catch((e) => utils.processError(e, res)),

  /**
   * Delete a task by name
   */
  delete: (req, res) =>
    services.tasks.remove(req.params.id)
      .then(() => res.status(HttpStatus.NO_CONTENT).json())
      .catch((e) => utils.processError(e, res)),

  /**
   * Get all errors for a given task
   */
  getErrors: (req, res) =>
    services.tasks.exists(req.params.id)
      .then((exists) =>
        exists
          ? services.tasks.errors(req.params.id).then((errors) => res.status(HttpStatus.OK).json(errors))
          : res.status(HttpStatus.NOT_FOUND).json({error: `task '${req.params.id}' not found`})
      )
      .catch((e) => utils.processError(e, res))
};