/*eslint no-magic-numbers: "off" */
const services = require('../../services');
const utils    = require('../../../config/utils');
const Task     = require('../../models/task');
const Promise  = require('bluebird');
const _        = require('lodash');

const getTaskStatus = (taskName)=> {
  const taskStatus = {};

  return services.manager.getBacklogCount(taskName).then(count => {
    taskStatus.backlog = count;
    return services.manager.getCompletedCount(taskName);
  }).then(count => {
    taskStatus.completed = count;
    return services.manager.getOverallProgress(taskName);
  }).then(overallProgress => {
    const inWork = _.reduce(overallProgress, (inner_result, progress) => {
      inner_result += progress.progress.total;
      return inner_result;
    }, 0);

    taskStatus.total           = taskStatus.backlog + taskStatus.completed + inWork;
    taskStatus.percentComplete = ((taskStatus.completed / taskStatus.total) * 100).toFixed(2);
    taskStatus.inProgress      = overallProgress;

    return taskStatus;
  });
};

/**
 * Returns list of all tasks, and their status/progress
 */
const getTasks = (req, res) => {
  try {
    services.manager.getTasks().then(taskNames => {
      return Promise.reduce(taskNames, (result, taskName) => {
        return getTaskStatus(taskName).then((status)=> {
          result[taskName] = status;
          return result;
        });
      }, {}).then(response => {
        res.status(200).json(response);
      }).catch(error => utils.processError(error, res));
    });
  } catch (error) {
    utils.processError(error, res);
  }
};


const getTask = (req, res) => {
  if (!Task.NAME_REGEX.test(req.params.id)) {
    res.status(400).json({error: 'task name must have alphanumeric characters only'});
    return;
  }

  const taskName = req.params.id;

  try {
    services.manager.taskExists(taskName).then(exists => {
      if (!exists) {
        res.status(404).json({error: `task '${taskName}' not found`});
        return;
      }

      return getTaskStatus(taskName).then(status => {
        res.status(200).json(status);
      });
    });
  } catch (error) {
    utils.processError(error, res);
  }
};

const addTask = (req, res)=> {
  if (!Task.NAME_REGEX.test(req.params.id)) {
    res.status(400).json({error: 'task name must have alphanumeric characters only'});
    return;
  }

  try {
    services.manager.addTask(req.params.id, req.body).then(()=> {
      res.status(200).json();
    });
  } catch (error) {
    utils.processError(error, res);
  }
};

const deleteTask = (req, res)=> {
  if (!Task.NAME_REGEX.test(req.params.id)) {
    res.status(400).json({error: 'task name must have alphanumeric characters only'});
    return;
  }

  try {
    services.manager.removeTask(req.params.id).then(()=> {
      res.status(204).json();
    });
  } catch (error) {
    utils.processError(error, res);
  }
};

const stop = (req, res)=> {
  services.manager.setRunning(false).then(()=> {
    res.status(200).json();
  }).catch(error => utils.processError(error, res));
};

const start = (req, res)=> {
  services.manager.setRunning(true).then(()=> {
    res.status(200).json();
  }).catch(error => utils.processError(error, res));
};

module.exports = {
  addTask:    addTask,
  deleteTask: deleteTask,
  getTasks:   getTasks,
  getTask:    getTask,
  start:      start,
  stop:       stop
};