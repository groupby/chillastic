const _         = require('lodash');
const moment    = require('moment');
const sillyname = require('sillyname');
const Promise   = require('bluebird');

const config = require('../../config/index');
const log    = config.log;

const RUN_KEY           = 'run';
const WORKER_NAME_KEY   = 'worker_name';
const WORKER_STATUS_KEY = 'worker_status';

let NAME_TIMEOUT_SEC = 10;

let redis = null;

/**
 * Manager constructor
 *
 * The manager prepares and 'manages' the jobs
 * @param sourceEs
 * @param redisClient
 * @constructor
 */
const Manager = function (redisClient) {
  const self = this;

  redis = redisClient;

  self.isRunning = isRunning;
  self.setRunning = setRunning;

  self.getWorkerName = reserveWorkerName;
  self.workerHeartbeat = workerHeartbeat;
  self.getWorkersStatus = getWorkersStatus;

  self._setWorkerName = setWorkerName;
  self._overrideNameTimeout = (timeout) => {
    NAME_TIMEOUT_SEC = timeout;
  };
};

/**
 * Recursively call the provided function to generate a worker name, until a name that has not been taken is generated.
 * @param getName
 * @returns {Promise.<TResult>}
 */
const setWorkerName = (getName) => {
  const name = getName();

  return purgeOldWorkerData()
  .then(() => redis.zadd(WORKER_NAME_KEY, 'NX', moment().valueOf(), name))
  .then((result) => result ? redis.hset(WORKER_STATUS_KEY, name, 'new').return(name) : setWorkerName(getName));
};

/**
 * Get the status of all workers
 *
 * @returns {*|{arity, flags, keyStart, keyStop, step}
 */
const getWorkersStatus = () =>
    purgeOldWorkerData()
    .then(() => redis.hgetall(WORKER_STATUS_KEY))
    .then((workersStatus) => _.reduce(workersStatus, (result, status, workerName) => _.assign(result, {[workerName]: JSON.parse(status)}), {}));

/**
 * Called by the workers to indicate they are alive
 *
 * @param name
 * @param status
 * @returns {Promise.<TResult>}
 */
const workerHeartbeat = (name, status) =>
    redis.zadd(WORKER_NAME_KEY, moment().valueOf(), name)
    .then(() => redis.hset(WORKER_STATUS_KEY, name, JSON.stringify(status)))
    .then(purgeOldWorkerData);

/**
 * Based on last heartbeat, removes any dead workers and their statuses.
 *
 * @returns {Promise.<TResult>}
 */
const purgeOldWorkerData = () =>
    redis.zremrangebyscore(WORKER_NAME_KEY, '-inf', moment().subtract(NAME_TIMEOUT_SEC, 'seconds').valueOf())
    .then(() => redis.zrangebyscore(WORKER_NAME_KEY, '-inf', '+inf'))
    .then((activeWorkerNames) => {
      log.debug(`Active workers: ${activeWorkerNames}`);

      return redis.hkeys(WORKER_STATUS_KEY)
      .then((allWorkerNames) => {
        log.debug(`All workers: ${allWorkerNames}`);
        return _.difference(allWorkerNames, activeWorkerNames);
      })
      .then((oldWorkerNames) => {
        if (oldWorkerNames.length > 0) {
          log.info(`Expiring status of workers: ${oldWorkerNames}`);
        }
        return Promise.each(oldWorkerNames, (oldName) => redis.hdel(WORKER_STATUS_KEY, oldName));
      });
    });

/**
 * Uses sillyname to reserve a unique name
 */
const reserveWorkerName = () => setWorkerName(sillyname);

/**
 * Return TRUE if workers are supposed to be running
 * @returns {Promise.<boolean>|*}
 */
const isRunning = () => redis.get(RUN_KEY).then((running) => running === 'running');

/**
 * Start/stop the workers from running
 * @param running
 * @returns {*}
 */
const setRunning = (running) => redis.set(RUN_KEY, running ? 'running' : 'stopped');

module.exports = Manager;
