const express    = require('express');
const bodyParser = require('bodyParser');
const controller = require('./root.controller');
const tasks      = require('./tasks');
const filters    = require('./filters');
const mutators   = require('./mutators');

const router = express.Router();
router.post('/_start', controller.start);
router.post('/_stop', controller.stop);
router.get('/status', controller.getStatus);

router.use(bodyParser.json());
router.use('/tasks', tasks());
router.use('/filters', filters());
router.use('/mutators', mutators());

module.exports = ()=> router;
